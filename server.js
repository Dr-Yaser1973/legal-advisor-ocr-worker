 /**
 * OCR Worker (Render) - Version 2.1 (Production)
 * - Supabase download
 * - Gemini primary (with timeout + retry)
 * - Tesseract fallback
 * - PDF -> PNG rendering
 * - Secure callback to Next.js
 * - Health endpoint
 */

import express from "express";
import cors from "cors";
import { createClient } from "@supabase/supabase-js";
import { GoogleGenerativeAI } from "@google/generative-ai";
import Tesseract from "tesseract.js";


// PDF -> Image rendering
import * as pdfjsLib from "pdfjs-dist/legacy/build/pdf.mjs";
import { createCanvas } from "canvas";

/* =========================
   App
========================= */
const app = express();

/* =========================
   Config / Env
========================= */
const PORT = Number(process.env.PORT || "10000");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const OCR_WORKER_SECRET = process.env.OCR_WORKER_SECRET;
const OCR_CALLBACK_URL = process.env.OCR_CALLBACK_URL || "";
const OCR_CALLBACK_SECRET = process.env.OCR_CALLBACK_SECRET || "";

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL = process.env.GEMINI_MODEL || "gemini-1.5-flash";

const MAX_PDF_PAGES = Math.max(1, Number(process.env.MAX_PDF_PAGES || "20"));

/* =========================
   Clients
========================= */
const supabase =
  SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
    ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
        auth: { persistSession: false },
      })
    : null;

const genAI = GEMINI_API_KEY
  ? new GoogleGenerativeAI(GEMINI_API_KEY)
  : null;

/* =========================
   Helpers
========================= */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const isPdf = (mimeType, p) =>
  mimeType === "application/pdf" ||
  (p || "").toLowerCase().endsWith(".pdf");

const cleanText = (t = "") =>
  t.replace(/\u0000/g, "")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();

/* =========================
   Secure Callback
========================= */
async function callbackNext(documentId, status, text, meta = {}) {
  if (!OCR_CALLBACK_URL) {
    console.warn("⚠️ No OCR_CALLBACK_URL set, skipping callback");
    return;
  }

  try {
    const res = await fetch(OCR_CALLBACK_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-callback-secret": OCR_CALLBACK_SECRET,
      },
      body: JSON.stringify({
        documentId,
        status,
        text,
        meta,
      }),
      timeout: 15_000,
    });

    if (!res.ok) {
      const t = await res.text();
      console.error("❌ Callback failed", res.status, t);
    } else {
      console.log("📡 Callback sent", { documentId, status });
    }
  } catch (e) {
    console.error("❌ Callback error", e?.message || e);
  }
}

/* =========================
   Gemini OCR (Timeout + Retry)
========================= */
async function geminiOCR({ buffer, mimeType, lang }) {
  if (!genAI) throw new Error("Gemini client not configured");

  const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });

  const uint8Data = new Uint8Array(buffer);
  const base64Data = Buffer.from(uint8Data).toString("base64");

  const prompt = `Extract text exactly as it appears. No summary. Preserve structure. Language: ${
    lang || "ar/en"
  }`;

  const req = {
    contents: [
      {
        role: "user",
        parts: [
          { text: prompt },
          {
            inlineData: {
              data: base64Data,
              mimeType: mimeType || "application/pdf",
            },
          },
        ],
      },
    ],
  };

  const maxAttempts = 3;
  let attempt = 0;

  while (attempt < maxAttempts) {
    attempt++;
    try {
      console.log(`🧠 GEMINI OCR attempt ${attempt}`);

      const result = await Promise.race([
        model.generateContent(req),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error("gemini_timeout")), 90_000)
        ),
      ]);

      const text = result?.response?.text?.() || "";

      return {
        text: cleanText(text),
        provider: "gemini",
        attempts: attempt,
      };
    } catch (e) {
      const msg = e?.message || String(e);
      const is429 =
        msg.includes("429") ||
        msg.toLowerCase().includes("quota") ||
        msg.toLowerCase().includes("rate");

      if (!is429 || attempt === maxAttempts) {
        console.warn("❌ Gemini OCR failed:", msg);
        throw e;
      }

      let delay = 2000 * Math.pow(2, attempt);
      const m = msg.match(/retry[^0-9]*([0-9.]+)/i);
      if (m) delay = Math.ceil(Number(m[1]) * 1000) + 1000;

      console.warn(
        `⚠️ Gemini rate limit, retrying in ${delay}ms (Attempt ${attempt})`
      );
      await sleep(delay);
    }
  }
}

/* =========================
   Tesseract OCR (Fallback)
========================= */
async function tesseractOCR({ buffer, mimeType, path, lang }) {
  const worker = await Tesseract.createWorker(
    lang?.includes("ar") ? "ara+eng" : "eng"
  );

  try {
    let fullText = "";

    if (isPdf(mimeType, path)) {
      const { pages } = await pdfToPngBuffers(buffer, MAX_PDF_PAGES);

      for (const pageImg of pages) {
        const {
          data: { text },
        } = await worker.recognize(new Uint8Array(pageImg));
        fullText += text + "\n";
      }
    } else {
      const {
        data: { text },
      } = await worker.recognize(new Uint8Array(buffer));
      fullText = text;
    }

    return {
      text: cleanText(fullText),
      provider: "tesseract",
    };
  } finally {
    await worker.terminate();
  }
}

/* =========================
   PDF → PNG
========================= */
async function pdfToPngBuffers(pdfBuffer, maxPages = 20) {
  const doc = await pdfjsLib
    .getDocument({ data: new Uint8Array(pdfBuffer) })
    .promise;

  const out = [];
  const pages = Math.min(doc.numPages, maxPages);

  for (let i = 1; i <= pages; i++) {
    const page = await doc.getPage(i);
    const viewport = page.getViewport({ scale: 2 });

    const canvas = createCanvas(viewport.width, viewport.height);
    await page.render({
      canvasContext: canvas.getContext("2d"),
      viewport,
    }).promise;

    out.push(canvas.toBuffer("image/png"));
  }

  return { pages: out };
}

/* =========================
   Middleware
========================= */
app.use(express.json({ limit: "25mb" }));
app.use(cors());

/* =========================
   Health
========================= */
app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    supabase: !!supabase,
    gemini: !!genAI,
    time: new Date().toISOString(),
  });
});

/* =========================
   Process Route
========================= */
app.post("/process", async (req, res) => {
  const { documentId, bucket, path, mimeType, lang } = req.body;

  if (req.headers["x-worker-secret"] !== OCR_WORKER_SECRET) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }

  console.log("📥 OCR JOB", { documentId, bucket, path });

  try {
    if (!supabase) throw new Error("Supabase client not configured");

    const { data, error } = await supabase.storage
      .from(bucket)
      .download(path);

    if (error) throw error;

    const fileBuffer = Buffer.from(await data.arrayBuffer());

    let result;

    try {
      result = await geminiOCR({
        buffer: fileBuffer,
        mimeType,
        lang,
      });
    } catch (e) {
      console.warn("⚠️ Gemini failed, fallback to Tesseract");
      result = await tesseractOCR({
        buffer: fileBuffer,
        mimeType,
        path,
        lang,
      });
    }

    await callbackNext(documentId, "COMPLETED", result.text, {
      provider: result.provider,
      attempts: result.attempts || 1,
      pages: isPdf(mimeType, path) ? "pdf" : "image",
    });

    console.log("✅ OCR DONE", {
      documentId,
      provider: result.provider,
    });

    res.json({ ok: true, ...result });
  } catch (e) {
    console.error("❌ OCR FAILED", e?.message || e);

    await callbackNext(documentId, "FAILED", null, {
      error: e?.message || "unknown",
    });

    res.status(502).json({ ok: false, error: e?.message || "OCR failed" });
  }
});

/* =========================
   Start
========================= */
app.listen(PORT, () => {
  console.log(`🚀 OCR Worker running on port ${PORT}`);
});
