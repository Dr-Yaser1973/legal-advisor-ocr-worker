 import express from "express";
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
app.use(express.json({ limit: "5mb" }));

// ---------------- ENV ----------------
const PORT = Number(process.env.PORT || "10000");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const OCR_WORKER_SECRET = process.env.OCR_WORKER_SECRET;

const OCR_CALLBACK_URL = process.env.OCR_CALLBACK_URL || null;
const OCR_CALLBACK_SECRET =
  process.env.OCR_CALLBACK_SECRET || OCR_WORKER_SECRET;

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL = process.env.GEMINI_MODEL || "gemini-1.5-flash";

const MAX_PDF_PAGES = Number(process.env.MAX_PDF_PAGES || "20");
const PDF_DPI = Number(process.env.PDF_DPI || "300");

const CALLBACK_TIMEOUT_MS = Number(process.env.CALLBACK_TIMEOUT_MS || "15000");
const MAX_CALLBACK_TEXT_CHARS = Number(process.env.MAX_CALLBACK_TEXT_CHARS || "20000");

// ✅ Production toggles
const FORCE_OCR = process.env.FORCE_OCR === "1";                 // يجبر OCR دائمًا
const FORCE_OCR_FOR_AR = process.env.FORCE_OCR_FOR_AR !== "0";   // الافتراضي true
const MIN_PDFTEXT_LEN = Number(process.env.MIN_PDFTEXT_LEN || "250"); // أقل طول لقبول pdftotext

// ---------------- Guards ----------------
function must(name, v) {
  if (!v) throw new Error(`Missing env: ${name}`);
}
must("SUPABASE_URL", SUPABASE_URL);
must("SUPABASE_SERVICE_ROLE_KEY", SUPABASE_SERVICE_ROLE_KEY);
must("OCR_WORKER_SECRET", OCR_WORKER_SECRET);
must("GEMINI_API_KEY", GEMINI_API_KEY);

// ---------------- Clients ----------------
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});
const genAI = new GoogleGenerativeAI(GEMINI_API_KEY);
const gemini = genAI.getGenerativeModel({ model: GEMINI_MODEL });

// ---------------- Utils ----------------
function log(...a) {
  console.log(new Date().toISOString(), ...a);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function tmpDir() {
  const d = path.join(os.tmpdir(), "ocr-" + crypto.randomBytes(8).toString("hex"));
  fs.mkdirSync(d, { recursive: true });
  return d;
}

function run(cmd, args, opts = {}) {
  return new Promise((res, rej) => {
    const p = spawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"], ...opts });
    let stderr = "";
    p.stderr.on("data", (d) => (stderr += d.toString()));
    p.on("close", (c) => {
      if (c === 0) res({ stderr });
      else rej(new Error(`${cmd} failed (${c}): ${stderr}`));
    });
  });
}

function clamp(n, min, max) {
  return Math.min(max, Math.max(min, n));
}

function hasArabic(text) {
  // نطاق العربية + العربية الممتدة
  return /[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]/.test(text || "");
}

function looksCorrupted(text) {
  if (!text) return true;

  const sample = (text || "").slice(0, 800);
  if (!sample.trim()) return true;

  // رموز فساد شائعة + أحرف التحكم
  const bad = (sample.match(/[�%#@]/g) || []).length;
  const ctrl = (sample.match(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g) || []).length;

  const ratioBad = (bad + ctrl) / Math.max(1, sample.length);

  // إذا فيها فساد بنسبة ملحوظة أو مليانة @@@
  if (ratioBad > 0.03) return true;
  if (/(@{8,}|%{5,}|#{5,})/.test(sample)) return true;

  return false;
}

// ---------------- Core ----------------
async function download(bucket, objectPath, out) {
  log("📥 Download", bucket, objectPath);
  const { data, error } = await supabase.storage.from(bucket).download(objectPath);
  if (error) throw new Error(error.message);
  const buf = Buffer.from(await data.arrayBuffer());
  await fsp.writeFile(out, buf);
}

// ✅ pdftotext مع UTF-8 + فحص قرار تجاوز OCR
async function tryPdfToText(pdfPath) {
  try {
    const { stdout } = await new Promise((res, rej) => {
      // -enc UTF-8 يقلّل مشاكل الترميز كثيرًا
      const p = spawn("pdftotext", ["-layout", "-enc", "UTF-8", pdfPath, "-"], {
        stdio: ["ignore", "pipe", "pipe"],
      });
      let out = "";
      let err = "";
      p.stdout.on("data", (d) => (out += d.toString("utf8")));
      p.stderr.on("data", (d) => (err += d.toString()));
      p.on("close", (c) => {
        if (c === 0) res({ stdout: out, stderr: err });
        else rej(new Error(err || "pdftotext failed"));
      });
    });

    const text = (stdout || "").trim();
    const normalizedLen = text.replace(/\s+/g, " ").length;

    if (normalizedLen < MIN_PDFTEXT_LEN) return null;

    // قرار الإنتاج:
    // - إذا FORCE_OCR → لا نستخدم pdftotext
    // - إذا نص عربي و FORCE_OCR_FOR_AR → لا نستخدم pdftotext (إلا إذا نظيف جدًا)
    // - إذا النص فاسد → لا نستخدم pdftotext
    if (FORCE_OCR) {
      log("🧾 pdftotext available but FORCE_OCR=1, running OCR");
      return null;
    }

    const ar = hasArabic(text);
    const corrupted = looksCorrupted(text);

    if (corrupted) {
      log("🧾 pdftotext produced corrupted text -> running OCR");
      return null;
    }

    if (ar && FORCE_OCR_FOR_AR) {
      // حتى لو غير فاسد، في ملفات عربية كثيرة “شكلها طبيعي” لكنها ناقصة/مقلوبة.
      // إذا تريد السماح للأداء، عطّل FORCE_OCR_FOR_AR=0.
      log("🧾 Arabic detected; FORCE_OCR_FOR_AR enabled -> running OCR");
      return null;
    }

    log("🧾 pdftotext clean — skipping OCR");
    return text;
  } catch {
    return null;
  }
}

async function pdfToImages(pdf, dir, pages) {
  const prefix = path.join(dir, "page");
  const max = clamp(Math.min(pages, MAX_PDF_PAGES), 1, MAX_PDF_PAGES);

  log("🖼️ PDF -> Images", { dpi: PDF_DPI, pages: max });

  // pdftoppm: ثابت وموثوق
  await run("pdftoppm", [
    "-png",
    "-r",
    String(PDF_DPI),
    "-f",
    "1",
    "-l",
    String(max),
    pdf,
    prefix,
  ]);

  const imgs = [];
  for (let i = 1; i <= max; i++) {
    const p = `${prefix}-${i}.png`;
    if (fs.existsSync(p)) imgs.push(p);
  }
  if (!imgs.length) throw new Error("No images generated");
  return imgs;
}

// ---------------- OCR Engines ----------------
async function geminiOCR(imgPath) {
  const b64 = (await fsp.readFile(imgPath)).toString("base64");

  const maxAttempts = 3;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const prompt = `
أنت نظام OCR قانوني عربي.
استخرج النص كما هو من الصورة بدقة عالية جدًا.
- بدون تلخيص
- بدون إعادة صياغة
- حافظ على الأرقام، عناوين المواد، الفقرات، والرموز.
- إذا كانت هناك جداول، أعدها كنص منسّق.
      `.trim();

      const result = await gemini.generateContent([
        { text: prompt },
        { inlineData: { mimeType: "image/png", data: b64 } },
      ]);

      const t = result?.response?.text?.().trim() || "";
      if (!t) return null;

      // إذا Gemini رجّع garbage (نادراً) نعيد null
      if (looksCorrupted(t)) return null;

      return t;
    } catch (e) {
      const msg = e?.message || String(e);
      const isRateLimit =
        msg.includes("429") || msg.toLowerCase().includes("quota");

      if (isRateLimit && attempt < maxAttempts) {
        let waitSec = 20 * attempt;
        const match = msg.match(/retry after ([\d.]+)s/i);
        if (match) waitSec = parseFloat(match[1]) + 2;

        log(`⚠️ Gemini quota hit. Attempt ${attempt}/${maxAttempts}, waiting ${waitSec}s`);
        await sleep(waitSec * 1000);
        continue;
      }

      log("❌ Gemini Error:", msg);
      return null;
    }
  }
  return null;
}

async function tesseractOCR(imgPath) {
  try {
    const { data } = await Tesseract.recognize(imgPath, "ara+eng", {
      // إعدادات محسنة
      tessedit_pageseg_mode: "6",
      preserve_interword_spaces: "1",
    });

    const t = (data?.text || "").trim();
    if (!t) return null;
    if (looksCorrupted(t)) return null;
    return t;
  } catch (e) {
    log("⚠️ Tesseract failed:", e?.message || e);
    return null;
  }
}

// ---------------- Callback ----------------
async function callback(url, payload) {
  if (!url) return;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), CALLBACK_TIMEOUT_MS);

  try {
    log("📡 Callback ->", url);

    const safePayload = { ...payload };

    if (typeof safePayload.text === "string") {
      safePayload.text = safePayload.text.slice(0, MAX_CALLBACK_TEXT_CHARS);
      safePayload.truncated = payload.text.length > MAX_CALLBACK_TEXT_CHARS;
    }

    const resp = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "x-worker-secret": OCR_CALLBACK_SECRET,
      },
      body: JSON.stringify(safePayload),
      signal: controller.signal,
    });

    if (!resp.ok) {
      const t = await resp.text().catch(() => "");
      log(`❌ Callback Error ${resp.status}: ${t}`);
    } else {
      log("✅ Callback OK");
    }
  } catch (e) {
    log("❌ Callback Exception:", e?.message || e);
  } finally {
    clearTimeout(timeout);
  }
}

// ---------------- Routes ----------------
app.get("/health", (_r, s) => {
  s.json({
    ok: true,
    version: "3.0",
    model: GEMINI_MODEL,
    dpi: PDF_DPI,
    maxPages: MAX_PDF_PAGES,
    forceOcr: FORCE_OCR,
    forceOcrForAr: FORCE_OCR_FOR_AR,
    minPdfTextLen: MIN_PDFTEXT_LEN,
  });
});

app.post("/ocr", async (req, res) => {
  const { documentId, bucket, objectPath, maxPages } = req.body || {};
  const jobId = crypto.randomBytes(6).toString("hex");

  try {
    if (req.headers["x-worker-secret"] !== OCR_WORKER_SECRET) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    if (!documentId || !bucket || !objectPath) {
      return res.status(400).json({ ok: false, error: "Missing fields" });
    }

    if (!String(objectPath).toLowerCase().endsWith(".pdf")) {
      return res.status(400).json({ ok: false, error: "Only PDF files supported" });
    }

    log("📄 JOB RECEIVED", { jobId, documentId, bucket, objectPath });

    // رد فوري لمنع Timeout
    res.json({ ok: true, jobId, message: "Processing started" });

    // المعالجة في الخلفية
    const dir = tmpDir();
    const pdf = path.join(dir, "input.pdf");

    try {
      await download(bucket, objectPath, pdf);

      // 1) جرّب استخراج نص مباشر (مع قرار إنتاجي)
      const directText = await tryPdfToText(pdf);
      if (directText) {
        await callback(OCR_CALLBACK_URL, {
          ok: true,
          jobId,
          documentId,
          engine: "pdftotext",
          pages: null,
          text: directText,
        });
        log("✅ JOB COMPLETED (pdftotext)", { jobId, documentId });
        return;
      }

      // 2) OCR على الصور
      const imgs = await pdfToImages(pdf, dir, Number(maxPages || MAX_PDF_PAGES));

      let fullText = "";
      for (const img of imgs) {
        log(`🔍 Page OCR`, { jobId, page: path.basename(img) });

        let t = await geminiOCR(img);
        if (!t) {
          log("⚠️ Fallback to Tesseract", { jobId, page: path.basename(img) });
          t = await tesseractOCR(img);
        }

        if (t) fullText += "\n\n" + t;
      }

      fullText = fullText.trim();
      if (!fullText) throw new Error("OCR generated no text");

      await callback(OCR_CALLBACK_URL, {
        ok: true,
        jobId,
        documentId,
        engine: "gemini+tesseract",
        pages: imgs.length,
        text: fullText,
      });

      log("✅ JOB COMPLETED (OCR)", { jobId, documentId, pages: imgs.length });
    } finally {
      await fsp.rm(dir, { recursive: true, force: true }).catch(() => {});
    }
  } catch (e) {
    log("❌ JOB FAILED", { jobId, documentId, error: e?.message || String(e) });

    await callback(OCR_CALLBACK_URL, {
      ok: false,
      jobId,
      documentId,
      error: e?.message || String(e),
    });
  }
});

// ---------------- Start ----------------
app.listen(PORT, () => {
  log(`🚀 OCR Worker Production v3 running on port ${PORT}`);
});
