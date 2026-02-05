 import express from "express";
import { createClient } from "@supabase/supabase-js";
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

const MAX_PDF_PAGES = Number(process.env.MAX_PDF_PAGES || "20");
const PDF_DPI = Number(process.env.PDF_DPI || "300");

const CALLBACK_TIMEOUT_MS = Number(process.env.CALLBACK_TIMEOUT_MS || "15000");
const MAX_CALLBACK_TEXT_CHARS = Number(
  process.env.MAX_CALLBACK_TEXT_CHARS || "20000"
);

// ---------------- Guards ----------------
function must(name, v) {
  if (!v) throw new Error(`Missing env: ${name}`);
}
must("SUPABASE_URL", SUPABASE_URL);
must("SUPABASE_SERVICE_ROLE_KEY", SUPABASE_SERVICE_ROLE_KEY);
must("OCR_WORKER_SECRET", OCR_WORKER_SECRET);

// ---------------- Clients ----------------
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

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

function looksCorrupted(text) {
  if (!text) return true;
  const s = text.slice(0, 800);
  const bad = (s.match(/[�%#@]/g) || []).length;
  return bad / Math.max(1, s.length) > 0.03;
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

  log("🖼️ PDF → Images", { dpi: PDF_DPI, pages: max });

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

// ---------------- Tesseract OCR ----------------
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
  log("🧠 Tesseract worker ready");
  return worker;
}

async function tesseractOCR(imgPath) {
  try {
    const worker = await getWorker();
    const { data } = await worker.recognize(imgPath);
    const text = (data?.text || "").trim();
    if (!text || looksCorrupted(text)) return null;
    return text;
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
  s.json({
    ok: true,
    service: "OCR Worker",
    engine: "tesseract",
    dpi: PDF_DPI,
    maxPages: MAX_PDF_PAGES,
  });
});

app.post("/ocr", async (req, res) => {
  const { documentId, bucket, objectPath, maxPages } = req.body;

  if (req.headers["x-worker-secret"] !== OCR_WORKER_SECRET) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }

  if (!documentId || !bucket || !objectPath) {
    return res.status(400).json({ ok: false, error: "Missing fields" });
  }

  log("📄 JOB RECEIVED", { documentId, objectPath });

  // رد فوري
  res.json({ ok: true, message: "Processing started" });

  const dir = tmpDir();
  const pdf = path.join(dir, "input.pdf");

  try {
    await download(bucket, objectPath, pdf);

    const imgs = await pdfToImages(
      pdf,
      dir,
      Number(maxPages || MAX_PDF_PAGES)
    );

    let fullText = "";

    for (const img of imgs) {
      log("🔍 OCR page", path.basename(img));
      const t = await tesseractOCR(img);
      if (t) fullText += "\n\n" + t;
    }

    fullText = fullText.trim();

    if (!fullText) {
      await callback(OCR_CALLBACK_URL, {
        ok: false,
        documentId,
        error: "OCR generated no text",
      });
      log("❌ JOB FAILED (no text)", documentId);
      return;
    }

    await callback(OCR_CALLBACK_URL, {
      ok: true,
      documentId,
      engine: "tesseract",
      pages: imgs.length,
      text: fullText,
    });

    log("✅ JOB COMPLETED", { documentId, pages: imgs.length });
  } catch (e) {
    log("❌ JOB ERROR", e.message);
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
  log(`🚀 OCR Worker (Tesseract-only) running on port ${PORT}`);
});
