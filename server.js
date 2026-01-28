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
        msg.toLowerCase().includes("quota");

      if (!is429 || attempt === maxAttempts) {
        throw e;
      }

      // Parse retryDelay like "retryDelay: '45s'" if present
      let delayMs = 1500 * attempt;
      const m = msg.match(/retryDelay[^0-9]*([0-9.]+)\s*s/i);
      if (m && m[1]) {
        delayMs = Math.ceil(Number(m[1]) * 1000);
      } else {
        // exponential backoff capped
        delayMs = Math.min(15000, 1500 * 2 ** (attempt - 1));
      }

      await sleep(delayMs);
    }
  }

  throw lastErr || new Error("Gemini OCR failed");
}

// --------------------------
// PDF -> PNG pages for Tesseract
// --------------------------
async function pdfToPngBuffers(pdfBuffer, maxPages = 20, scale = 2) {
  const doc = await pdfjsLib.getDocument({ data: pdfBuffer }).promise;
  const total = doc.numPages;
  const pages = Math.min(total, maxPages);

  const out = [];
  for (let i = 1; i <= pages; i++) {
    const page = await doc.getPage(i);
    const viewport = page.getViewport({ scale });
    const canvas = createCanvas(viewport.width, viewport.height);
    const ctx = canvas.getContext("2d");

    await page.render({
      canvasContext: ctx,
      viewport,
    }).promise;

    const png = canvas.toBuffer("image/png");
    out.push(png);
  }
  return { pages: out, totalPages: total, usedPages: pages };
}

// --------------------------
// Tesseract OCR (fallback)
// --------------------------
async function tesseractOCR({ buffer, mimeType, path, lang }) {
  const tessLang = guessLangPack(lang);

  const worker = await Tesseract.createWorker(tessLang, 1, {
    logger: () => {},
  });

  try {
    let full = "";

    // If PDF => render to images
    if (isPdf(mimeType, path)) {
      const { pages, totalPages, usedPages } = await pdfToPngBuffers(
        buffer,
        MAX_PDF_PAGES,
        2
      );

      for (let i = 0; i < pages.length; i++) {
        const r = await worker.recognize(pages[i]);
        full += (r?.data?.text || "") + "\n";
      }

      return {
        text: cleanText(full),
        provider: "tesseract",
        meta: { totalPages, usedPages },
      };
    }

    // Otherwise treat buffer as image
    const r = await worker.recognize(buffer);
    return { text: cleanText(r?.data?.text || ""), provider: "tesseract" };
  } finally {
    await worker.terminate();
  }
}

// --------------------------
// Main OCR pipeline
// --------------------------
async function runOCRJob(job) {
  const {
    documentId,
    bucket,
    path,
    mimeType,
    lang = "ar",
    // optional: if you already pass a signedUrl instead of storage path
    signedUrl,
  } = job || {};

  if (!documentId) throw new Error("documentId is required");
  if (!bucket && !signedUrl) throw new Error("bucket is required (or signedUrl)");
  if (!path && !signedUrl) throw new Error("path is required (or signedUrl)");

  // 1) Download file
  let fileBuffer;
  if (signedUrl) {
    const resp = await fetch(signedUrl);
    if (!resp.ok) throw new Error(`signedUrl download failed: ${resp.status}`);
    const ab = await resp.arrayBuffer();
    fileBuffer = Buffer.from(ab);
  } else {
    fileBuffer = await downloadFromSupabase(bucket, path);
  }

  // 2) Try Gemini first
  if (GEMINI_API_KEY) {
    try {
      const g = await geminiOCR({
        buffer: fileBuffer,
        mimeType: mimeType || (isPdf(mimeType, path) ? "application/pdf" : "image/png"),
        lang,
      });

      // if Gemini returns very small text (often a sign of failure on scanned docs)
      if ((g.text || "").length >= 30) {
        return { ok: true, ...g };
      }
      // otherwise fall through to Tesseract
    } catch (e) {
      // fall back
      console.error("⚠️ Gemini failed -> fallback to Tesseract:", e?.message || e);
    }
  }

  // 3) Fallback Tesseract
  const t = await tesseractOCR({
    buffer: fileBuffer,
    mimeType,
    path,
    lang,
  });

  if (!t.text || t.text.length < 10) {
    throw new Error("OCR produced empty/too-short output");
  }

  return { ok: true, ...t };
}

// --------------------------
// Routes
// --------------------------
app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "ocr-worker",
    time: new Date().toISOString(),
    gemini: Boolean(GEMINI_API_KEY),
    supabase: Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY),
  });
});

/**
 * POST /process
 * Headers:
 *  - x-worker-secret: OCR_WORKER_SECRET
 *
 * Body:
 *  {
 *    "documentId": 44,
 *    "bucket": "library-documents",
 *    "path": "translations/req-44/source.pdf",
 *    "mimeType": "application/pdf",
 *    "lang": "ar",
 *    "signedUrl": "optional"
 *  }
 *
 * Response:
 *  { ok: true, text, provider, meta? }
 */
app.post("/process", requireSecret, async (req, res) => {
  const rid = req._rid;
  const started = Date.now();

  try {
    const job = req.body || {};
    console.log(`[${rid}] ✅ job received`, {
      documentId: job.documentId,
      bucket: job.bucket,
      path: job.path,
      mimeType: job.mimeType,
      hasSignedUrl: Boolean(job.signedUrl),
    });

    const result = await runOCRJob(job);

    const ms = Date.now() - started;

    // Optional callback to Next.js
    // (This is helpful if you want worker to push results without waiting in Next)
    const cb = await postCallback({
      documentId: job.documentId,
      ok: true,
      text: result.text,
      provider: result.provider,
      meta: result.meta || null,
      elapsedMs: ms,
    });

    console.log(`[${rid}] ✅ done in ${ms}ms via ${result.provider}`, {
      textLen: result.text?.length || 0,
      callback: cb?.ok ? "ok" : "failed/skipped",
    });

    return res.json({
      ok: true,
      text: result.text,
      provider: result.provider,
      meta: result.meta || null,
      elapsedMs: ms,
      callback: cb,
    });
  } catch (e) {
    const ms = Date.now() - started;

    const message = e?.message || String(e);
    console.error(`[${rid}] ❌ failed in ${ms}ms`, message);

    // Optional callback failure
    await postCallback({
      documentId: req.body?.documentId || null,
      ok: false,
      error: message,
      elapsedMs: ms,
    });

    // Return error (Bad Gateway-like for upstream)
    return res.status(502).json({
      ok: false,
      error: message,
      elapsedMs: ms,
    });
  }
});

// --------------------------
// Start
// --------------------------
app.listen(PORT, () => {
  console.log(`✅ OCR Worker listening on :${PORT}`);
});
