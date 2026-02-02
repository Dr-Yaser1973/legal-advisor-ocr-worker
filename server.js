 /**
 * OCR Worker (Render) - Production
 * Primary: Gemini (image->text)
 * Fallback: Tesseract.js (ara+eng)
 *
 * ENV (Required):
 *   PORT=10000
 *   SUPABASE_URL=...
 *   SUPABASE_SERVICE_ROLE_KEY=...
 *   OCR_WORKER_SECRET=some-strong-secret
 *   GEMINI_API_KEY=...
 *
 * ENV (Recommended):
 *   OCR_CALLBACK_URL=https://your-next-app.vercel.app/api/ocr/worker/callback
 *   OCR_CALLBACK_SECRET=some-other-secret (or reuse OCR_WORKER_SECRET)
 *   GEMINI_MODEL=gemini-1.5-flash
 *   MAX_PDF_PAGES=20
 *   PDF_DPI=300
 *   CALLBACK_TIMEOUT_MS=15000
 *
 * Request:
 *   POST /ocr
 *   {
 *     documentId: number|string,
 *     bucket: "library" | "...",
 *     objectPath: "laws/xxx.pdf",
 *     maxPages?: number,
 *     callbackUrl?: string // optional override
 *   }
 */

import express from "express";
import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";
import { GoogleGenerativeAI } from "@google/generative-ai";
import { spawn } from "child_process";
import fs from "fs";
import fsp from "fs/promises";
import path from "path";
import os from "os";
import crypto from "crypto";
import * as Tesseract from "tesseract.js";

const app = express();
app.use(express.json({ limit: "2mb" }));

// -------------------------
// ENV & Clients
// -------------------------
const PORT = Number(process.env.PORT || "10000");
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const OCR_WORKER_SECRET = process.env.OCR_WORKER_SECRET;

const OCR_CALLBACK_URL = process.env.OCR_CALLBACK_URL; // optional but recommended
const OCR_CALLBACK_SECRET = process.env.OCR_CALLBACK_SECRET || OCR_WORKER_SECRET;

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL = process.env.GEMINI_MODEL || "gemini-1.5-flash";

const MAX_PDF_PAGES = Number(process.env.MAX_PDF_PAGES || "20");
const PDF_DPI = Number(process.env.PDF_DPI || "300");
const CALLBACK_TIMEOUT_MS = Number(process.env.CALLBACK_TIMEOUT_MS || "15000");

function requireEnv(name, value) {
  if (!value) throw new Error(`Missing required env: ${name}`);
}

requireEnv("SUPABASE_URL", SUPABASE_URL);
requireEnv("SUPABASE_SERVICE_ROLE_KEY", SUPABASE_SERVICE_ROLE_KEY);
requireEnv("OCR_WORKER_SECRET", OCR_WORKER_SECRET);
requireEnv("GEMINI_API_KEY", GEMINI_API_KEY);

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const genAI = new GoogleGenerativeAI(GEMINI_API_KEY);
const gemini = genAI.getGenerativeModel({ model: GEMINI_MODEL });

// -------------------------
// Helpers
// -------------------------
function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function safeNumber(x) {
  const n = typeof x === "string" ? Number(x) : x;
  return Number.isFinite(n) ? n : null;
}

function makeTempDir(prefix = "ocr-worker-") {
  const dir = path.join(os.tmpdir(), prefix + crypto.randomBytes(8).toString("hex"));
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function run(cmd, args, opts = {}) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"], ...opts });
    let stdout = "";
    let stderr = "";
    p.stdout.on("data", (d) => (stdout += d.toString()));
    p.stderr.on("data", (d) => (stderr += d.toString()));
    p.on("error", reject);
    p.on("close", (code) => {
      if (code === 0) return resolve({ stdout, stderr });
      reject(new Error(`${cmd} failed (code=${code})\n${stderr || stdout}`));
    });
  });
}

async function downloadFromSupabase(bucket, objectPath, outFile) {
  log("📥 Download from Supabase", { bucket, path: objectPath });

  const { data, error } = await supabase.storage.from(bucket).download(objectPath);
  if (error) throw new Error(`Supabase download error: ${error.message}`);

  const arrayBuffer = await data.arrayBuffer();
  const buf = Buffer.from(arrayBuffer);
  await fsp.writeFile(outFile, buf);
  return outFile;
}

async function tryExtractTextPdftotext(pdfPath) {
  // إذا كان PDF نصي، هذا أفضل من OCR
  try {
    const { stdout } = await run("pdftotext", ["-layout", pdfPath, "-"]);
    const text = (stdout || "").trim();
    // اعتبره نصيًا إذا لديه قدر معقول
    if (text && text.replace(/\s+/g, " ").length >= 200) {
      return text;
    }
    return null;
  } catch {
    return null;
  }
}

async function convertPdfToImages(pdfPath, outDir, maxPages) {
  // نستخدم pdftoppm لأنه ثابت ومتوفر ضمن poppler-utils
  // -r DPI لضمان صور كبيرة كفاية
  const prefix = path.join(outDir, "page");
  const pages = Math.min(Math.max(1, maxPages), MAX_PDF_PAGES);

  log("🖼️ Converting PDF -> PNG images", { dpi: PDF_DPI, pages });

  // pdftoppm لا يدعم تحديد عدد الصفحات مباشرة في كل الإصدارات بنفس الطريقة،
  // لذا نستخدم -f و -l
  const args = [
    "-png",
    "-r",
    String(PDF_DPI),
    "-f",
    "1",
    "-l",
    String(pages),
    pdfPath,
    prefix,
  ];

  await run("pdftoppm", args);

  // ملفات مثل: page-1.png, page-2.png ...
  const images = [];
  for (let i = 1; i <= pages; i++) {
    const p = `${prefix}-${i}.png`;
    if (fs.existsSync(p)) images.push(p);
  }

  if (!images.length) throw new Error("No images generated from PDF (pdftoppm output empty).");
  return images;
}

async function geminiOcrImage(imagePath) {
  const b = await fsp.readFile(imagePath);
  const base64 = b.toString("base64");

  const prompt = `
أنت نظام OCR قانوني محترف.
استخرج النص العربي/الإنكليزي من الصورة كما هو بدقة عالية.
- حافظ على ترتيب الأسطر والفقرات قدر الإمكان
- لا تفسّر ولا تلخّص
- إذا وجد عنوان/مادة/رقم فقرة فاحتفظ به كما هو
أعد النص فقط دون أي شروحات.
`.trim();

  const result = await gemini.generateContent([
    { text: prompt },
    {
      inlineData: {
        mimeType: "image/png",
        data: base64,
      },
    },
  ]);

  const text = result?.response?.text?.() || "";
  return text.trim() || null;
}

async function tesseractOcrImage(imagePath) {
  const { data } = await Tesseract.recognize(imagePath, "ara+eng", {
    // قيم مفيدة للعربي
    tessedit_pageseg_mode: "6",
    preserve_interword_spaces: "1",
  });

  const text = (data?.text || "").trim();
  return text || null;
}

async function ocrImages(images) {
  // 1) Gemini محاولة أولى (ممتاز للعربي)
  // 2) fallback إلى Tesseract عند الفشل أو خروج نص فارغ

  const pageTexts = [];
  for (let i = 0; i < images.length; i++) {
    const img = images[i];
    log(`🔎 OCR page ${i + 1}/${images.length}`, path.basename(img));

    let text = null;

    // Gemini
    try {
      text = await geminiOcrImage(img);
      if (text && text.length > 10) {
        pageTexts.push(text);
        continue;
      }
      log("⚠️ Gemini returned empty/too short text; fallback to Tesseract");
    } catch (e) {
      log("⚠️ Gemini error; fallback to Tesseract:", e?.message || e);
    }

    // Tesseract fallback
    try {
      const t = await tesseractOcrImage(img);
      pageTexts.push(t || "");
    } catch (e) {
      log("❌ Tesseract failed:", e?.message || e);
      pageTexts.push("");
    }
  }

  const combined = pageTexts.join("\n\n").trim();
  return combined || null;
}

async function postCallback(callbackUrl, payload) {
  if (!callbackUrl) {
    log("ℹ️ Callback skipped (no callbackUrl configured)");
    return;
  }

  let urlObj;
  try {
    urlObj = new URL(callbackUrl);
  } catch {
    throw new Error(`Invalid callbackUrl: ${callbackUrl}`);
  }

  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), CALLBACK_TIMEOUT_MS);

  try {
    const res = await fetch(urlObj.toString(), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-worker-secret": OCR_CALLBACK_SECRET || OCR_WORKER_SECRET,
      },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });

    const txt = await res.text().catch(() => "");
    if (!res.ok) {
      throw new Error(`Callback status ${res.status}: ${txt.slice(0, 400)}`);
    }

    log("✅ Callback ok", { status: res.status });
  } finally {
    clearTimeout(t);
  }
}

// -------------------------
// Routes
// -------------------------
app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "legal-advisor-ocr-worker", model: GEMINI_MODEL });
});

// Auth middleware for worker endpoint
function requireWorkerSecret(req, res, next) {
  const h = req.headers["x-worker-secret"];
  if (!h || h !== OCR_WORKER_SECRET) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }
  next();
}

app.post("/ocr", requireWorkerSecret, async (req, res) => {
  const startedAt = Date.now();

  const documentId = safeNumber(req.body?.documentId);
  const bucket = (req.body?.bucket || "").trim();
  const objectPath = (req.body?.objectPath || "").trim();
  const maxPagesReq = safeNumber(req.body?.maxPages) || MAX_PDF_PAGES;

  const callbackUrl =
    (req.body?.callbackUrl && String(req.body.callbackUrl).trim()) ||
    (OCR_CALLBACK_URL && OCR_CALLBACK_URL.trim()) ||
    null;

  if (!documentId || !bucket || !objectPath) {
    return res.status(400).json({
      ok: false,
      error: "Missing required fields: documentId, bucket, objectPath",
    });
  }

  log("📄 JOB RECEIVED", { documentId, bucket, objectPath, maxPages: maxPagesReq });

  // Respond fast (optional): you can keep it synchronous too.
  // We'll process synchronously so you can see errors immediately.
  const tempDir = makeTempDir();
  const pdfFile = path.join(tempDir, "input.pdf");

  try {
    await downloadFromSupabase(bucket, objectPath, pdfFile);

    // 1) If PDF has real text, use it directly
    const directText = await tryExtractTextPdftotext(pdfFile);
    if (directText) {
      log("🧾 PDF contains extractable text (pdftotext) - skipping OCR");
      const payload = {
        ok: true,
        documentId,
        bucket,
        objectPath,
        engine: "pdftotext",
        text: directText,
        pages: null,
        tookMs: Date.now() - startedAt,
      };

      // callback
      if (callbackUrl) {
        log("📡 Callback ->", callbackUrl);
        await postCallback(callbackUrl, payload);
      }

      return res.json({ ok: true, mode: "pdftotext", length: directText.length });
    }

    // 2) Otherwise: PDF -> images -> OCR
    const images = await convertPdfToImages(pdfFile, tempDir, maxPagesReq);

    // quick sanity check for tiny images (your old issue)
    // we can read PNG header? We'll just log file sizes
    for (const p of images.slice(0, 3)) {
      const st = await fsp.stat(p);
      log("🧩 Image sample", { file: path.basename(p), bytes: st.size });
    }

    const text = await ocrImages(images);
    if (!text) {
      throw new Error("OCR produced empty text (Gemini+Tesseract).");
    }

    const payload = {
      ok: true,
      documentId,
      bucket,
      objectPath,
      engine: "gemini_primary_tesseract_fallback",
      model: GEMINI_MODEL,
      dpi: PDF_DPI,
      pages: images.length,
      text,
      tookMs: Date.now() - startedAt,
    };

    if (callbackUrl) {
      log("📡 Callback ->", callbackUrl);
      await postCallback(callbackUrl, payload);
    }

    return res.json({
      ok: true,
      pages: images.length,
      length: text.length,
      tookMs: payload.tookMs,
    });
  } catch (e) {
    const message = e?.message || String(e);
    log("❌ JOB FAILED", { documentId, error: message });

    // Even on fail, notify callback (if exists) so DB can mark FAILED
    try {
      if (callbackUrl) {
        log("📡 Callback(fail) ->", callbackUrl);
        await postCallback(callbackUrl, {
          ok: false,
          documentId,
          bucket,
          objectPath,
          error: message,
          tookMs: Date.now() - startedAt,
        });
      }
    } catch (cbErr) {
      log("❌ CALLBACK ERROR", cbErr?.message || cbErr);
    }

    return res.status(500).json({ ok: false, error: message });
  } finally {
    // cleanup
    try {
      await fsp.rm(tempDir, { recursive: true, force: true });
    } catch {}
  }
});

// -------------------------
// Start
// -------------------------
app.listen(PORT, () => {
  log(`🚀 OCR Worker running on port ${PORT}`);
});
