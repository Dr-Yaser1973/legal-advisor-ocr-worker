/**
 * OCR Worker (Render)
 * - Receives OCR jobs from Next.js via OCR_SERVICE_URL
 * - Downloads file from Supabase Storage
 * - Runs Gemini (primary) with retry on 429
 * - Falls back to Tesseract (secondary) for scanned / when Gemini fails
 * - Optionally posts callback to Next.js to update DB status
 *
 * Required ENV:
 *   PORT=10000 (Render default ok)
 *   SUPABASE_URL=...
 *   SUPABASE_SERVICE_ROLE_KEY=...
 *   OCR_WORKER_SECRET=some-strong-secret
 *   GEMINI_API_KEY=...
 *
 * Optional ENV:
 *   OCR_CALLBACK_URL=https://your-next-app.com/api/ocr/worker/callback
 *   OCR_CALLBACK_SECRET=another-secret (recommended, separate from OCR_WORKER_SECRET)
 *   GEMINI_MODEL=gemini-1.5-flash (or your chosen)
 *   MAX_PDF_PAGES=20
 */

import express from "express";
import cors from "cors";
import crypto from "crypto";
import { createClient } from "@supabase/supabase-js";
import { GoogleGenerativeAI } from "@google/generative-ai";
import Tesseract from "tesseract.js";

// PDF -> Image rendering (fallback path)
import * as pdfjsLib from "pdfjs-dist/legacy/build/pdf.mjs";
import { createCanvas } from "canvas";

const app = express();

// --------------------------
// Config / Env
// --------------------------
const PORT = Number(process.env.PORT || "10000");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const OCR_WORKER_SECRET = process.env.OCR_WORKER_SECRET;

const OCR_CALLBACK_URL = process.env.OCR_CALLBACK_URL || "";
const OCR_CALLBACK_SECRET = process.env.OCR_CALLBACK_SECRET || "";

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL = process.env.GEMINI_MODEL || "gemini-1.5-flash";

const MAX_PDF_PAGES = Math.max(1, Number(process.env.MAX_PDF_PAGES || "20"));

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("❌ Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
}
if (!OCR_WORKER_SECRET) {
  console.error("❌ Missing OCR_WORKER_SECRET");
}
if (!GEMINI_API_KEY) {
  console.error("⚠️ Missing GEMINI_API_KEY (Gemini primary will fail; fallback may still work)");
}

// --------------------------
// Middlewares
// --------------------------
app.use(express.json({ limit: "10mb" }));
app.use(
  cors({
    origin: true,
    credentials: true,
  })
);

// Simple request id for logs
app.use((req, _res, next) => {
  req._rid = crypto.randomBytes(4).toString("hex");
  next();
});

// Secret auth
function requireSecret(req, res, next) {
  const got = req.headers["x-worker-secret"];
  if (!got || got !== OCR_WORKER_SECRET) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }
  next();
}

// --------------------------
// Clients
// --------------------------
const supabase =
  SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
    ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
        auth: { persistSession: false },
      })
    : null;

const genAI = GEMINI_API_KEY ? new GoogleGenerativeAI(GEMINI_API_KEY) : null;

// --------------------------
// Helpers
// --------------------------
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isPdf(mimeType, path) {
  if (mimeType === "application/pdf") return true;
  return (path || "").toLowerCase().endsWith(".pdf");
}

function guessLangPack(lang) {
  // Support: "ar", "en", "ara", "eng", "ar-en"
  const l = (lang || "").toLowerCase();
  if (l.includes("ar") && l.includes("en")) return "ara+eng";
  if (l.includes("ar")) return "ara";
  return "eng"; // default
}

function cleanText(t) {
  return (t || "")
    .replace(/\u0000/g, "")
    .replace(/\r\n/g, "\n")
    .replace(/[ \t]+\n/g, "\n")
    .trim();
}

async function postCallback(payload) {
  if (!OCR_CALLBACK_URL) return { ok: true, skipped: true };

  try {
    const headers = {
      "content-type": "application/json",
    };
    // optional separate secret
    if (OCR_CALLBACK_SECRET) headers["x-callback-secret"] = OCR_CALLBACK_SECRET;

    const r = await fetch(OCR_CALLBACK_URL, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });

    const text = await r.text().catch(() => "");
    if (!r.ok) {
      return { ok: false, status: r.status, body: text };
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e?.message || String(e) };
  }
}

// --------------------------
// Supabase download
// --------------------------
async function downloadFromSupabase(bucket, path) {
  if (!supabase) throw new Error("Supabase client not configured");
  const { data, error } = await supabase.storage.from(bucket).download(path);
  if (error) throw new Error(`Supabase download failed: ${error.message}`);
  const arrayBuffer = await data.arrayBuffer();
  return Buffer.from(arrayBuffer);
}

// --------------------------
// Gemini OCR (primary) with retry
// --------------------------
function buildGeminiPrompt(lang) {
  const l = (lang || "").toLowerCase();
  const isAr = l.includes("ar");
  // Keep prompt short and strict to reduce tokens & hallucinations
  return isAr
    ? `
أنت نظام OCR احترافي.
استخرج النص حرفياً كما يظهر في المستند.
- لا تشرح ولا تلخص.
- حافظ على ترتيب الأسطر والعناوين قدر الإمكان.
- إن وُجدت جداول اكتبها كنص منسق.
أعد النص فقط دون أي مقدمة.
`.trim()
    : `
You are a professional OCR system.
Extract the text exactly as it appears.
- Do not explain or summarize.
- Preserve line order and headings as much as possible.
- If tables exist, output them as structured text.
Return ONLY the extracted text.
`.trim();
}

async function geminiOCR({ buffer, mimeType, lang }) {
  if (!genAI) throw new Error("Gemini client not configured");

  const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });

  // Gemini accepts inlineData (base64)
  const base64 = buffer.toString("base64");
  const prompt = buildGeminiPrompt(lang);

  const req = {
    contents: [
      {
        role: "user",
        parts: [
          { text: prompt },
          {
            inlineData: {
              data: base64,
              mimeType: mimeType || "application/pdf",
            },
          },
        ],
      },
    ],
  };

  // Retry on 429 using exponential + optional retryDelay if available in error string
  const maxAttempts = 3;
  let attempt = 0;
  let lastErr = null;

  while (attempt < maxAttempts) {
    attempt++;
    try {
      const result = await model.generateContent(req);
      const text = result?.response?.text?.() || "";
      return { text: cleanText(text), provider: "gemini", attempts: attempt };
    } catch (e) {
      lastErr = e;

      const msg = e?.message || String(e);
      const is429 =
        msg.includes("429") ||
        msg.toLowerCase().includes("too many requests") ||
        msg.toLowerCase

        // --------------------------
// Start
// --------------------------
app.listen(PORT, () => {
  console.log(`✅ OCR Worker listening on :${PORT}`);
});
