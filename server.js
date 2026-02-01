 /**
 * Legal Advisor OCR Worker — Production (Linked)
 * ----------------------------------------------
 * Flow:
 * Next.js -> POST /job
 *   headers: x-worker-secret
 *   body:
 *     {
 *       documentId: number,
 *       fileUrl: string (SIGNED URL from Supabase),
 *       callbackUrl: string
 *     }
 *
 * Worker:
 *  - downloads file from signed URL
 *  - PDF -> images (pdftoppm)
 *  - OCR via Tesseract (ara+eng)
 *  - POST callback to Next.js
 *
 * ENV REQUIRED:
 * PORT=10000
 * OCR_WORKER_SECRET
 *
 * Docker must include:
 *  - poppler-utils
 *  - tesseract-ocr (+ ara + eng)
 */

import express from "express";
import fs from "fs";
import path from "path";
import fetch from "node-fetch";
import { execSync } from "child_process";
import Tesseract from "tesseract.js";
import tmp from "tmp";

// ===============================
// ENV
// ===============================
const PORT = Number(process.env.PORT || "10000");
const OCR_WORKER_SECRET = process.env.OCR_WORKER_SECRET;

// ===============================
// Guards
// ===============================
if (!OCR_WORKER_SECRET) {
  throw new Error("OCR_WORKER_SECRET missing");
}

// ===============================
// App
// ===============================
const app = express();
app.use(express.json({ limit: "50mb" }));

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

// ===============================
// Health
// ===============================
app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

// ===============================
// Utils
// ===============================
function normalizeArabic(text = "") {
  return text
    .replace(/\u0640/g, "")
    .replace(/[^\u0600-\u06FF0-9\s.,\-()]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

async function downloadFile(url, outFile) {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`Download failed: ${res.status}`);
  }
  const buffer = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(outFile, buffer);
}

// ===============================
// OCR Core
// ===============================
async function tesseractOCR(pdfPath, maxPages = 20) {
  const tmpDir = tmp.dirSync({ unsafeCleanup: true });
  const prefix = path.join(tmpDir.name, "page");

  // PDF -> PNG (Linux native)
  execSync(`pdftoppm -png "${pdfPath}" "${prefix}"`, {
    stdio: "ignore",
  });

  const images = fs
    .readdirSync(tmpDir.name)
    .filter((f) => f.startsWith("page") && f.endsWith(".png"))
    .slice(0, maxPages)
    .map((f) => path.join(tmpDir.name, f));

  let fullText = "";

  for (const img of images) {
    const res = await Tesseract.recognize(img, "ara+eng", {
      tessedit_pageseg_mode: 6,
    });
    fullText += "\n" + (res?.data?.text || "");
  }

  tmpDir.removeCallback();
  return {
    pages: images.length,
    text: normalizeArabic(fullText),
  };
}

// ===============================
// OCR JOB
// ===============================
app.post("/job", async (req, res) => {
  try {
    const secret = String(req.headers["x-worker-secret"] || "");
    if (secret !== OCR_WORKER_SECRET) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    const { documentId, fileUrl, callbackUrl, maxPages } = req.body || {};

    if (
      !documentId ||
      !Number.isFinite(Number(documentId)) ||
      !fileUrl ||
      !callbackUrl
    ) {
      return res
        .status(400)
        .json({ ok: false, error: "Invalid payload" });
    }

    log("📥 JOB", { documentId });

    const tmpFile = tmp.fileSync({ postfix: ".pdf" }).name;

    // Download from Supabase signed URL
    await downloadFile(fileUrl, tmpFile);

    let ok = true;
    let engine = "TESSERACT";
    let result = { text: "", pages: 0 };

    try {
      result = await tesseractOCR(
        tmpFile,
        Number(maxPages || 20)
      );
    } catch (e) {
      ok = false;
      log("❌ OCR FAILED", e?.message || e);
    }

    try {
      fs.unlinkSync(tmpFile);
    } catch {}

    // Callback to Next.js
    await fetch(callbackUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        documentId: Number(documentId),
        ok,
        engine,
        text: result.text || null,
        pages: result.pages || null,
        isScanned: true,
      }),
    });

    res.json({ ok: true });
  } catch (e) {
    log("❌ JOB ERROR", e?.message || e);
    res.status(500).json({ ok: false, error: e?.message || "Job failed" });
  }
});

// ===============================
// Start
// ===============================
app.listen(PORT, "0.0.0.0", () => {
  log("🚀 OCR Worker running on", PORT);
});
