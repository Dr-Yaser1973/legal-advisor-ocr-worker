 /**
 * Legal Advisor OCR Worker (Production)
 * ------------------------------------
 * Flow:
 * Next.js -> POST /job
 *  - validates secret
 *  - downloads file from Supabase
 *  - tries Gemini OCR (primary)
 *  - fallback to Tesseract (PDF -> Images -> OCR)
 *  - POST callback to Next.js
 *
 * ENV REQUIRED:
 * PORT=10000
 * SUPABASE_URL
 * SUPABASE_SERVICE_ROLE_KEY
 * OCR_WORKER_SECRET
 * GEMINI_API_KEY
 *
 * OPTIONAL:
 * OCR_CALLBACK_URL=https://your-next-app.com/api/ocr/worker/callback
 * OCR_CALLBACK_SECRET=same-or-different-secret
 * GEMINI_MODEL=gemini-1.5-flash
 * MAX_PDF_PAGES=20
 */

import express from "express";
import fetch from "node-fetch";
import fs from "fs";
import path from "path";
import tmp from "tmp";
import { createClient } from "@supabase/supabase-js";
import { GoogleGenerativeAI } from "@google/generative-ai";
import Tesseract from "tesseract.js";
import { convert } from "pdf-poppler";

// ===============================
// ENV
// ===============================
const PORT = process.env.PORT || 10000;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const OCR_WORKER_SECRET = process.env.OCR_WORKER_SECRET;
const OCR_CALLBACK_URL = process.env.OCR_CALLBACK_URL;
const OCR_CALLBACK_SECRET =
  process.env.OCR_CALLBACK_SECRET || OCR_WORKER_SECRET;

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL =
  process.env.GEMINI_MODEL || "gemini-1.5-flash";

const MAX_PDF_PAGES = Number(process.env.MAX_PDF_PAGES || "20");

// ===============================
// Guards
// ===============================
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY missing");
}
if (!OCR_WORKER_SECRET) {
  throw new Error("OCR_WORKER_SECRET missing");
}

// ===============================
// Clients
// ===============================
const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
);

const genAI = GEMINI_API_KEY
  ? new GoogleGenerativeAI(GEMINI_API_KEY)
  : null;

// ===============================
// Utils
// ===============================
function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function normalizeArabic(text) {
  return text
    .replace(/\u0640/g, "")
    .replace(/[^\u0600-\u06FF0-9\s.,\-()]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

async function sendCallback(payload) {
  if (!OCR_CALLBACK_URL) return;

  try {
    await fetch(OCR_CALLBACK_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-callback-secret": OCR_CALLBACK_SECRET
      },
      body: JSON.stringify(payload)
    });
    log("📡 Callback sent", payload);
  } catch (e) {
    log("❌ Callback failed", e.message);
  }
}

// ===============================
// Supabase Download
// ===============================
async function downloadFromSupabase(bucket, filePath, outFile) {
  const { data, error } = await supabase.storage
    .from(bucket)
    .download(filePath);

  if (error || !data) {
    throw new Error("Supabase download failed: " + error?.message);
  }

  const buffer = Buffer.from(await data.arrayBuffer());
  fs.writeFileSync(outFile, buffer);
}

// ===============================
// Gemini OCR
// ===============================
async function geminiOCR(textHint, base64File) {
  if (!genAI) throw new Error("Gemini disabled");

  const model = genAI.getGenerativeModel({
    model: GEMINI_MODEL
  });

  const prompt = `
أنت نظام OCR قانوني محترف.
استخرج النص العربي والإنجليزي من المستند بدقة.
رتّب الفقرات بشكل واضح.
لا تضف شرحًا.
`;

  const result = await model.generateContent([
    { text: prompt },
    {
      inlineData: {
        mimeType: "application/pdf",
        data: base64File
      }
    }
  ]);

  return result.response.text();
}

// ===============================
// PDF -> Images -> Tesseract
// ===============================
async function tesseractOCR(pdfPath) {
  const tmpDir = tmp.dirSync({ unsafeCleanup: true });

  await convert(pdfPath, {
    format: "png",
    out_dir: tmpDir.name,
    out_prefix: "page",
    page: null
  });

  const images = fs
    .readdirSync(tmpDir.name)
    .filter((f) => f.endsWith(".png"))
    .slice(0, MAX_PDF_PAGES)
    .map((f) => path.join(tmpDir.name, f));

  let fullText = "";

  for (const img of images) {
    const res = await Tesseract.recognize(
      img,
      "ara+eng",
      {
        tessedit_pageseg_mode: 6
      }
    );
    fullText += "\n" + res.data.text;
  }

  tmpDir.removeCallback();
  return fullText;
}

// ===============================
// Express
// ===============================
const app = express();
app.use(express.json({ limit: "50mb" }));

// ===============================
// Health
// ===============================
app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    gemini: Boolean(genAI),
    callback: Boolean(OCR_CALLBACK_URL)
  });
});

// ===============================
// OCR JOB
// ===============================
app.post("/job", async (req, res) => {
  try {
    const secret = req.headers["x-worker-secret"];
    if (secret !== OCR_WORKER_SECRET) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    const { documentId, bucket, path: filePath } = req.body;

    if (!documentId || !bucket || !filePath) {
      return res
        .status(400)
        .json({ ok: false, error: "Invalid job payload" });
    }

    log("📥 OCR JOB", { documentId, bucket, filePath });

    const tmpFile = tmp.fileSync({ postfix: ".pdf" }).name;

    await downloadFromSupabase(bucket, filePath, tmpFile);

    let text = "";
    let engine = "NONE";
    let ok = true;

    // ===============================
    // Try Gemini
    // ===============================
    try {
      log("🧠 GEMINI OCR attempt");
      const base64 = fs.readFileSync(tmpFile).toString("base64");
      text = await geminiOCR("", base64);
      engine = "GEMINI";
    } catch (e) {
      log("⚠ Gemini failed, fallback to Tesseract", e.message);
      try {
        text = await tesseractOCR(tmpFile);
        engine = "TESSERACT";
      } catch (err) {
        ok = false;
        text = "";
        log("❌ Tesseract failed", err.message);
      }
    }

    fs.unlinkSync(tmpFile);

    text = normalizeArabic(text);

    await sendCallback({
      documentId,
      ok,
      engine,
      text,
      pages: null,
      isScanned: engine === "TESSERACT"
    });

    res.json({ ok: true });
  } catch (e) {
    log("❌ JOB ERROR", e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ===============================
// Start
// ===============================
app.listen(PORT, () => {
  log("🚀 OCR Worker running on port", PORT);
});
