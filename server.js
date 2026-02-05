 import express from "express";
import { createClient } from "@supabase/supabase-js";
import { GoogleGenerativeAI } from "@google/generative-ai";
import { spawn } from "child_process";
import fs from "fs";
import fsp from "fs/promises";
import path from "path";
import os from "os";
import crypto from "crypto";
import { createWorker } from "tesseract.js";

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
const GEMINI_MODEL = process.env.GEMINI_MODEL || null;

const MAX_PDF_PAGES = Number(process.env.MAX_PDF_PAGES || "20");
const PDF_DPI = Number(process.env.PDF_DPI || "300");

const CALLBACK_TIMEOUT_MS = Number(process.env.CALLBACK_TIMEOUT_MS || "15000");
const MAX_CALLBACK_TEXT_CHARS = Number(
  process.env.MAX_CALLBACK_TEXT_CHARS || "20000"
);

const FORCE_OCR_FOR_AR = process.env.FORCE_OCR_FOR_AR !== "0";

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

function run(cmd, args) {
  return new Promise((res, rej) => {
    const p = spawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"] });
    let err = "";
    p.stderr.on("data", (d) => (err += d.toString()));
    p.on("close", (c) => {
      if (c === 0) res();
      else rej(new Error(`${cmd} failed: ${err}`));
    });
  });
}

function hasArabic(text) {
  return /[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]/.test(text || "");
}

function looksCorrupted(text) {
  if (!text) return true;
  const s = text.slice(0, 800);
  const bad = (s.match(/[�%#@]/g) || []).length;
  return bad / Math.max(1, s.length) > 0.03;
}

// ---------------- Gemini Model Resolver ----------------
let geminiModel = null;

async function resolveGeminiModelName() {
  const preferred = GEMINI_MODEL;
  const url = `https://generativelanguage.googleapis.com/v1beta/models?key=${GEMINI_API_KEY}`;
  try {
    const r = await fetch(url);
    const j = await r.json();
    const models = (j.models || []).map((m) => m.name);

    const candidates = [
      preferred && `models/${preferred}`,
      "models/gemini-1.5-pro",
      "models/gemini-1.0-pro-vision",
      "models/gemini-pro-vision",
      "models/gemini-pro",
    ].filter(Boolean);

    const found = candidates.find((c) => models.includes(c));
    return found ? found.replace("models/", "") : "gemini-1.5-pro";
  } catch {
    return preferred || "gemini-1.5-pro";
  }
}

async function getGeminiModel() {
  if (geminiModel) return geminiModel;
  const name = await resolveGeminiModelName();
  geminiModel = genAI.getGenerativeModel({ model: name });
  log("🧠 Gemini model selected:", name);
  return geminiModel;
}

// ---------------- Core ----------------
async function download(bucket, objectPath, out) {
  const { data, error } = await supabase.storage
    .from(bucket)
    .download(objectPath);
  if (error) throw new Error(error.message);
  const buf = Buffer.from(await data.arrayBuffer());
  await fsp.writeFile(out, buf);
}

async function tryPdfToText(pdfPath) {
  try {
    const { stdout } = await new Promise((res, rej) => {
      const p = spawn("pdftotext", ["-layout", "-enc", "UTF-8", pdfPath, "-"]);
      let out = "";
      p.stdout.on("data", (d) => (out += d.toString("utf8")));
      p.on("close", (c) => (c === 0 ? res({ stdout: out }) : rej()));
    });

    const text = stdout.trim();
    if (text.length < 250) return null;
    if (hasArabic(text) && FORCE_OCR_FOR_AR) return null;
    if (looksCorrupted(text)) return null;

    log("🧾 pdftotext clean — skipping OCR");
    return text;
  } catch {
    return null;
  }
}

async function pdfToImages(pdf, dir, pages) {
  const prefix = path.join(dir, "page");
  await run("pdftoppm", [
    "-png",
    "-r",
    String(PDF_DPI),
    "-f",
    "1",
    "-l",
    String(pages),
    pdf,
    prefix,
  ]);
  return Array.from({ length: pages }, (_, i) => `${prefix}-${i + 1}.png`).filter(
    fs.existsSync
  );
}

// ---------------- OCR Engines ----------------
async function geminiOCR(imgPath) {
  const model = await getGeminiModel();
  const b64 = (await fsp.readFile(imgPath)).toString("base64");

  try {
    const result = await model.generateContent([
      { text: "استخرج النص كما هو من الصورة بدقة عالية بدون تلخيص أو تفسير." },
      { inlineData: { mimeType: "image/png", data: b64 } },
    ]);
    const t = result?.response?.text?.().trim();
    return t && !looksCorrupted(t) ? t : null;
  } catch (e) {
    log("❌ Gemini Error:", e.message);
    return null;
  }
}

// ---------------- Tesseract (v5 correct usage) ----------------
let tesseractWorker = null;

async function getWorker() {
  if (tesseractWorker) return tesseractWorker;

  const worker = await createWorker();
  await worker.loadLanguage("ara+eng");
  await worker.initialize("ara+eng");
  await worker.setParameters({
    tessedit_pageseg_mode: "6",
    preserve_interword_spaces: "1",
  });

  tesseractWorker = worker;
  return worker;
}

async function tesseractOCR(imgPath) {
  try {
    const worker = await getWorker();
    const { data } = await worker.recognize(imgPath);
    const t = (data?.text || "").trim();
    return t && !looksCorrupted(t) ? t : null;
  } catch (e) {
    log("⚠️ Tesseract failed:", e.message);
    return null;
  }
}

// ---------------- Callback ----------------
async function callback(url, payload) {
  if (!url) return;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), CALLBACK_TIMEOUT_MS);

  try {
    const safe = { ...payload };
    if (typeof safe.text === "string") {
      safe.text = safe.text.slice(0, MAX_CALLBACK_TEXT_CHARS);
    }

    await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "x-worker-secret": OCR_CALLBACK_SECRET,
      },
      body: JSON.stringify(safe),
      signal: controller.signal,
    });

    log("✅ Callback OK");
  } catch (e) {
    log("❌ Callback Error:", e.message);
  } finally {
    clearTimeout(timeout);
  }
}

// ---------------- Routes ----------------
app.get("/health", (_r, s) => {
  s.json({ ok: true, service: "OCR Worker", dpi: PDF_DPI });
});

app.post("/ocr", async (req, res) => {
  const { documentId, bucket, objectPath } = req.body;

  if (req.headers["x-worker-secret"] !== OCR_WORKER_SECRET) {
    return res.status(401).json({ ok: false });
  }

  res.json({ ok: true });

  const dir = tmpDir();
  const pdf = path.join(dir, "input.pdf");

  try {
    await download(bucket, objectPath, pdf);

    const direct = await tryPdfToText(pdf);
    if (direct) {
      await callback(OCR_CALLBACK_URL, {
        ok: true,
        documentId,
        engine: "pdftotext",
        text: direct,
      });
      return;
    }

    const imgs = await pdfToImages(pdf, dir, MAX_PDF_PAGES);
    let fullText = "";

    for (const img of imgs) {
      let t = await geminiOCR(img);
      if (!t) t = await tesseractOCR(img);
      if (t) fullText += "\n\n" + t;
    }

    if (!fullText.trim()) {
      await callback(OCR_CALLBACK_URL, {
        ok: false,
        documentId,
        error: "OCR generated no text",
      });
      return;
    }

    await callback(OCR_CALLBACK_URL, {
      ok: true,
      documentId,
      engine: "gemini+tesseract",
      pages: imgs.length,
      text: fullText.trim(),
    });
  } catch (e) {
    await callback(OCR_CALLBACK_URL, {
      ok: false,
      documentId,
      error: e.message,
    });
  } finally {
    await fsp.rm(dir, { recursive: true, force: true }).catch(() => {});
  }
});

// ---------------- Start ----------------
app.listen(PORT, () => {
  log(`🚀 OCR Worker (production) running on port ${PORT}`);
});
