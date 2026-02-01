 /**
 * Legal Advisor OCR Worker — Production (Supabase Direct Download)
 * ---------------------------------------------------------------
 * Next.js -> POST /job
 *   headers: x-worker-secret
 *   body:
 *     {
 *       documentId: number,
 *       bucket: string,        // e.g. "library"
 *       path: string,          // e.g. "laws/xxxx.pdf"
 *       callbackUrl: string,   // e.g. https://yourapp.com/api/ocr/worker/callback
 *       maxPages?: number      // default 20
 *     }
 *
 * Worker:
 *  - downloads file from Supabase Storage using SERVICE ROLE
 *  - PDF -> images via pdftoppm
 *  - OCR via Tesseract (ara+eng)
 *  - POST callback to Next.js with results
 *
 * ENV REQUIRED:
 *   PORT=10000
 *   OCR_WORKER_SECRET
 *   SUPABASE_URL
 *   SUPABASE_SERVICE_ROLE_KEY
 *
 * Docker deps:
 *   - poppler-utils
 *   - tesseract-ocr + ara + eng
 */

import express from "express";
import fs from "fs";
import path from "path";
import { execSync } from "child_process";
import tmp from "tmp";
import Tesseract from "tesseract.js";
import { createClient } from "@supabase/supabase-js";

// -------------------------------
// ENV
// -------------------------------
const PORT = Number(process.env.PORT || "10000");
const OCR_WORKER_SECRET = process.env.OCR_WORKER_SECRET;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!OCR_WORKER_SECRET) throw new Error("OCR_WORKER_SECRET missing");
if (!SUPABASE_URL) throw new Error("SUPABASE_URL missing");
if (!SUPABASE_SERVICE_ROLE_KEY) throw new Error("SUPABASE_SERVICE_ROLE_KEY missing");

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// -------------------------------
// App
// -------------------------------
const app = express();
app.use(express.json({ limit: "50mb" }));

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

// -------------------------------
// Health
// -------------------------------
app.get("/health", (_req, res) => res.json({ ok: true }));

// -------------------------------
// Utils
// -------------------------------
function normalizeArabic(text = "") {
  return text
    .replace(/\u0640/g, "") // tatweel
    .replace(/[^\u0600-\u06FF0-9\s.,\-()]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

async function downloadFromSupabase(bucket, objectPath, outFile) {
  log("📦 Download from Supabase", { bucket, path: objectPath });

  const { data, error } = await supabase.storage.from(bucket).download(objectPath);
  if (error || !data) {
    throw new Error(`Supabase download failed: ${error?.message || "no data"}`);
  }

  const buffer = Buffer.from(await data.arrayBuffer());
  fs.writeFileSync(outFile, buffer);
}

function pdfToPngs(pdfPath, outDir, maxPages) {
  const prefix = path.join(outDir, "page");

  // pdftoppm outputs: page-1.png, page-2.png ...
   execSync(
  `pdftoppm -r 300 -scale-to 2000 -png "${pdfPath}" "${prefix}"`,
  {
    stdio: "ignore",
  }
);


  const images = fs
    .readdirSync(outDir)
    .filter((f) => f.startsWith("page-") && f.endsWith(".png"))
    .sort((a, b) => {
      // sort by numeric page
      const na = Number(a.replace(/[^\d]/g, "")) || 0;
      const nb = Number(b.replace(/[^\d]/g, "")) || 0;
      return na - nb;
    })
    .slice(0, maxPages)
    .map((f) => path.join(outDir, f));

  return images;
}

async function runTesseractOnImages(images) {
  let fullText = "";

  for (const img of images) {
     const res = await Tesseract.recognize(img, "ara+eng", {
  tessedit_pageseg_mode: 4, // نص متعدد الأعمدة
  preserve_interword_spaces: "1",
});

    fullText += "\n" + (res?.data?.text || "");
  }

  return normalizeArabic(fullText);
}

async function postCallback(callbackUrl, payload) {
  log("📤 Callback ->", callbackUrl);

  const r = await fetch(callbackUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  const txt = await r.text().catch(() => "");
  if (!r.ok) {
    throw new Error(`Callback failed: ${r.status} ${txt}`);
  }
}

// -------------------------------
// OCR JOB
// -------------------------------
app.post("/job", async (req, res) => {
  const secret = String(req.headers["x-worker-secret"] || "");
  if (secret !== OCR_WORKER_SECRET) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }

  const { documentId, bucket, path: objectPath, callbackUrl, maxPages } = req.body || {};

  if (!documentId || !Number.isFinite(Number(documentId))) {
    return res.status(400).json({ ok: false, error: "documentId invalid" });
  }
  if (!bucket || typeof bucket !== "string") {
    return res.status(400).json({ ok: false, error: "bucket required" });
  }
  if (!objectPath || typeof objectPath !== "string") {
    return res.status(400).json({ ok: false, error: "path required" });
  }
  if (!callbackUrl || typeof callbackUrl !== "string") {
    return res.status(400).json({ ok: false, error: "callbackUrl required" });
  }

  const MAX = Math.min(50, Math.max(1, Number(maxPages || 20)));

  log("🧾 JOB RECEIVED", { documentId: Number(documentId), bucket, objectPath, maxPages: MAX });

  // respond early (optional) — لكن الأفضل نُنهي job ثم نرد
  // سنكمل ونرد في النهاية

  let ok = true;
  let engine = "TESSERACT";
  let text = "";
  let pages = 0;
  let errorMsg = null;

  // temp workspace
  const tmpPdf = tmp.fileSync({ postfix: ".pdf" });
  const tmpDir = tmp.dirSync({ unsafeCleanup: true });

  try {
    // 1) Download
    await downloadFromSupabase(bucket, objectPath, tmpPdf.name);

    // 2) Convert PDF -> images
    const images = pdfToPngs(tmpPdf.name, tmpDir.name, MAX);
    pages = images.length;

    if (!pages) {
      throw new Error("No pages generated from PDF (pdftoppm produced 0 images)");
    }

    // 3) OCR
    text = await runTesseractOnImages(images);
    if (!text || text.length < 5) {
      // لا نعتبره فشل، لكن نسجل تحذير
      log("⚠️ OCR produced very small text");
    }
  } catch (e) {
    ok = false;
    errorMsg = e?.message || String(e);
    log("❌ JOB ERROR", errorMsg);
  }

  // cleanup
  try { tmpPdf.removeCallback(); } catch {}
  try { tmpDir.removeCallback(); } catch {}

  // 4) Callback to Next.js
  try {
    await postCallback(callbackUrl, {
      documentId: Number(documentId),
      ok,
      engine,
      text: text || null,
      pages: pages || null,
      isScanned: true,
      error: errorMsg,
    });
  } catch (e) {
    ok = false;
    errorMsg = e?.message || String(e);
    log("❌ CALLBACK ERROR", errorMsg);
  }

  return res.json({ ok: true, received: true });
});

// -------------------------------
// Start
// -------------------------------
app.listen(PORT, "0.0.0.0", () => {
  log("🚀 OCR Worker running on port", PORT);
});
