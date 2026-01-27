import express from "express";
import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";
import Tesseract from "tesseract.js";

// ================================
// ENV
// ================================
const PORT = process.env.PORT || 10000;

const {
  OCR_WORKER_SECRET,
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  GEMINI_API_KEY,
  GEMINI_MODEL = "gemini-2.0-flash",
  OCR_LOG_LEVEL = "info",
} = process.env;

if (!OCR_WORKER_SECRET) {
  console.error("❌ OCR_WORKER_SECRET missing");
  process.exit(1);
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("❌ SUPABASE env missing");
  process.exit(1);
}
if (!GEMINI_API_KEY) {
  console.warn("⚠️ GEMINI_API_KEY missing — will fallback to Tesseract only");
}

// ================================
// Setup
// ================================
const app = express();
app.use(express.json({ limit: "20mb" }));

const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
);

// ================================
// Utils
// ================================
function log(...args) {
  if (OCR_LOG_LEVEL === "info") {
    console.log(new Date().toISOString(), ...args);
  }
}

function requireAuth(req, res) {
  const incoming = req.headers["x-worker-secret"];
  if (!incoming || incoming !== OCR_WORKER_SECRET) {
    log("❌ Unauthorized request", {
      incoming,
      expected: OCR_WORKER_SECRET,
    });
    res.status(401).json({ error: "Unauthorized" });
    return false;
  }
  return true;
}

async function downloadFromSupabase(bucket, path) {
  log("📥 Downloading from Supabase", { bucket, path });

  const { data, error } = await supabase.storage
    .from(bucket)
    .download(path);

  if (error) throw error;

  const arrayBuffer = await data.arrayBuffer();
  return Buffer.from(arrayBuffer);
}

// ================================
// OCR Engines
// ================================
async function runGeminiOCR(buffer, lang = "ar+en") {
  if (!GEMINI_API_KEY) return null;

  log("🤖 Running Gemini OCR");

  const base64 = buffer.toString("base64");

  const res = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent?key=${GEMINI_API_KEY}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [
          {
            role: "user",
            parts: [
              {
                text: `Extract all readable text from this PDF. Language preference: ${lang}. Return plain text only.`,
              },
              {
                inlineData: {
                  mimeType: "application/pdf",
                  data: base64,
                },
              },
            ],
          },
        ],
      }),
    }
  );

  if (!res.ok) {
    const t = await res.text();
    throw new Error("Gemini OCR failed: " + t);
  }

  const json = await res.json();
  return (
    json?.candidates?.[0]?.content?.parts?.[0]?.text || null
  );
}

async function runTesseractOCR(buffer, lang = "ara+eng") {
  log("🧠 Running Tesseract OCR");

  const {
    data: { text },
  } = await Tesseract.recognize(buffer, lang, {
    logger: (m) => log("TESSERACT:", m.status),
  });

  return text;
}

// ================================
// Routes
// ================================
app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "ocr-worker" });
});

// POST /work
// body: { bucket, path, language, callbackUrl, documentId }
app.post("/work", async (req, res) => {
  if (!requireAuth(req, res)) return;

  try {
    const {
      bucket = "library",
      path,
      language = "ar+en",
      callbackUrl,
      documentId,
    } = req.body || {};

    if (!path || !callbackUrl || !documentId) {
      return res.status(400).json({
        error: "bucket, path, callbackUrl, documentId are required",
      });
    }

    log("🚀 OCR Job received", {
      bucket,
      path,
      documentId,
    });

    const fileBuffer = await downloadFromSupabase(bucket, path);

    let extractedText = null;

    try {
      extractedText = await runGeminiOCR(fileBuffer, language);
      if (!extractedText || extractedText.trim().length < 10) {
        log("⚠️ Gemini returned empty, fallback to Tesseract");
        extractedText = await runTesseractOCR(fileBuffer);
      }
    } catch (e) {
      log("⚠️ Gemini failed, fallback to Tesseract", e.message);
      extractedText = await runTesseractOCR(fileBuffer);
    }

    if (!extractedText || extractedText.trim().length === 0) {
      throw new Error("OCR returned empty text");
    }

    log("✅ OCR completed, sending back to Next.js");

    // Callback to Next.js
    const cbRes = await fetch(callbackUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-worker-secret": OCR_WORKER_SECRET,
      },
      body: JSON.stringify({
        documentId,
        text: extractedText,
        pageCount: null,
        engine: "hybrid",
      }),
    });

    if (!cbRes.ok) {
      const t = await cbRes.text();
      throw new Error("Callback failed: " + t);
    }

    return res.json({
      ok: true,
      documentId,
      length: extractedText.length,
    });
  } catch (e) {
    log("❌ OCR ERROR:", e.message);
    return res.status(500).json({
      ok: false,
      error: e.message,
    });
  }
});

// ================================
// Start
// ================================
app.listen(PORT, () => {
  log(`🟢 OCR Worker running on port ${PORT}`);
});
