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
app.use(express.json({ limit: "2mb" }));

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

// ---------------- Core ----------------
async function download(bucket, objectPath, out) {
  log("📥 Download", bucket, objectPath);
  const { data, error } = await supabase.storage
    .from(bucket)
    .download(objectPath);
  if (error) throw new Error(error.message);
  const buf = Buffer.from(await data.arrayBuffer());
  await fsp.writeFile(out, buf);
}

async function pdfToImages(pdf, dir, pages) {
  const prefix = path.join(dir, "page");
  const max = Math.min(pages, MAX_PDF_PAGES);

  log("🖼️ PDF -> Images", { dpi: PDF_DPI, pages: max });

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

async function geminiOCR(imgPath) {
  const b64 = (await fsp.readFile(imgPath)).toString("base64");
  const result = await gemini.generateContent([
    { text: "استخرج النص من الصورة بدقة، بدون تلخيص أو تفسير." },
    {
      inlineData: { mimeType: "image/png", data: b64 },
    },
  ]);
  return result?.response?.text?.().trim() || null;
}

async function tesseractOCR(imgPath) {
  const { data } = await Tesseract.recognize(imgPath, "ara+eng", {
    tessedit_pageseg_mode: "6",
    preserve_interword_spaces: "1",
  });
  return (data?.text || "").trim() || null;
}

async function callback(url, payload) {
  if (!url) return;
  log("📡 Callback ->", url);

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-worker-secret": OCR_CALLBACK_SECRET,
    },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Callback failed ${res.status}: ${t}`);
  }
}

// ---------------- Routes ----------------
app.get("/health", (_r, s) => {
  s.json({ ok: true, model: GEMINI_MODEL });
});

app.post("/ocr", async (req, res) => {
  try {
    if (req.headers["x-worker-secret"] !== OCR_WORKER_SECRET) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    const { documentId, bucket, objectPath, maxPages } = req.body || {};
    if (!documentId || !bucket || !objectPath) {
      return res.status(400).json({ ok: false, error: "Missing fields" });
    }

    log("📄 JOB", { documentId, bucket, objectPath });

    const dir = tmpDir();
    const pdf = path.join(dir, "input.pdf");

    await download(bucket, objectPath, pdf);
    const imgs = await pdfToImages(pdf, dir, Number(maxPages || MAX_PDF_PAGES));

    let fullText = "";
    for (const img of imgs) {
      let t = await geminiOCR(img);
      if (!t) t = await tesseractOCR(img);
      if (t) fullText += "\n\n" + t;
    }

    if (!fullText.trim()) throw new Error("OCR empty");

    const payload = {
      ok: true,
      documentId,
      bucket,
      objectPath,
      text: fullText.trim(),
      engine: "gemini+tesseract",
      pages: imgs.length,
    };

    await callback(OCR_CALLBACK_URL, payload);

    res.json({ ok: true, pages: imgs.length });
  } catch (e) {
    log("❌ ERROR", e.message || e);
    try {
      await callback(OCR_CALLBACK_URL, {
        ok: false,
        documentId: req.body?.documentId,
        error: e.message || String(e),
      });
    } catch {}
    res.status(500).json({ ok: false, error: e.message || "failed" });
  }
});

// ---------------- Start ----------------
app.listen(PORT, () => {
  log(`🚀 OCR Worker running on port ${PORT}`);
});
