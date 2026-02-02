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

const CALLBACK_TIMEOUT_MS = Number(
  process.env.CALLBACK_TIMEOUT_MS || "15000"
);

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

async function tryPdfToText(pdfPath) {
  try {
    const { stdout } = await new Promise((res, rej) => {
      const p = spawn("pdftotext", ["-layout", pdfPath, "-"], {
        stdio: ["ignore", "pipe", "pipe"],
      });
      let out = "";
      let err = "";
      p.stdout.on("data", (d) => (out += d.toString()));
      p.stderr.on("data", (d) => (err += d.toString()));
      p.on("close", (c) => {
        if (c === 0) res({ stdout: out, stderr: err });
        else rej(new Error(err));
      });
    });

    const text = (stdout || "").trim();
    if (text && text.replace(/\s+/g, " ").length > 200) {
      log("🧾 pdftotext success — skipping OCR");
      return text;
    }
    return null;
  } catch {
    return null;
  }
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

// ---------------- OCR Engines ----------------
async function geminiOCR(imgPath) {
  const b64 = (await fsp.readFile(imgPath)).toString("base64");

  const maxAttempts = 3;
  let attempt = 0;

  while (attempt < maxAttempts) {
    attempt++;
    try {
      const result = await gemini.generateContent([
        { text: "استخرج النص من الصورة بدقة عالية بدون تلخيص أو تفسير." },
        { inlineData: { mimeType: "image/png", data: b64 } },
      ]);

      return result?.response?.text?.().trim() || null;
    } catch (e) {
      const msg = e.message || String(e);
      const isRateLimit =
        msg.includes("429") || msg.toLowerCase().includes("quota");

      if (isRateLimit && attempt < maxAttempts) {
        let waitSec = 30 * attempt;
        const match = msg.match(/retry after ([\d.]+)s/i);
        if (match) waitSec = parseFloat(match[1]) + 2;

        log(`⚠️ Gemini quota hit. Attempt ${attempt}, waiting ${waitSec}s`);
        await sleep(waitSec * 1000);
        continue;
      }

      log("❌ Gemini Error:", msg);
      return null;
    }
  }
}

async function tesseractOCR(imgPath) {
  try {
    const { data } = await Tesseract.recognize(imgPath, "ara+eng", {
      tessedit_pageseg_mode: "6",
      preserve_interword_spaces: "1",
    });
    return (data?.text || "").trim() || null;
  } catch (e) {
    log("⚠️ Tesseract failed:", e.message);
    return null;
  }
}

// ---------------- Callback ----------------
async function callback(url, payload) {
  if (!url) return;

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    CALLBACK_TIMEOUT_MS
  );

  try {
    log("📡 Callback ->", url);

    const safePayload = { ...payload };

    if (typeof safePayload.text === "string") {
      safePayload.text =
        safePayload.text.slice(0, MAX_CALLBACK_TEXT_CHARS);
      safePayload.truncated =
        payload.text.length > MAX_CALLBACK_TEXT_CHARS;
    }

    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-worker-secret": OCR_CALLBACK_SECRET,
      },
      body: JSON.stringify(safePayload),
      signal: controller.signal,
    });

    if (!res.ok) {
      const t = await res.text();
      log(`❌ Callback Error ${res.status}: ${t}`);
    } else {
      log("✅ Callback OK");
    }
  } catch (e) {
    log("❌ Callback Exception:", e.message || e);
  } finally {
    clearTimeout(timeout);
  }
}

// ---------------- Routes ----------------
app.get("/health", (_r, s) => {
  s.json({
    ok: true,
    model: GEMINI_MODEL,
    dpi: PDF_DPI,
    maxPages: MAX_PDF_PAGES,
  });
});

app.post("/ocr", async (req, res) => {
  const { documentId, bucket, objectPath, maxPages } = req.body || {};

  try {
    if (req.headers["x-worker-secret"] !== OCR_WORKER_SECRET) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    if (!documentId || !bucket || !objectPath) {
      return res.status(400).json({ ok: false, error: "Missing fields" });
    }

    if (!objectPath.toLowerCase().endsWith(".pdf")) {
      return res
        .status(400)
        .json({ ok: false, error: "Only PDF files supported" });
    }

    log("📄 JOB RECEIVED", { documentId, objectPath });

    // رد فوري لمنع Timeout
    res.json({ ok: true, message: "Processing started" });

    // المعالجة في الخلفية
    const dir = tmpDir();
    const pdf = path.join(dir, "input.pdf");

    try {
      await download(bucket, objectPath, pdf);

      // 1) جرّب استخراج نص مباشر
      const directText = await tryPdfToText(pdf);
      if (directText) {
        await callback(OCR_CALLBACK_URL, {
          ok: true,
          documentId,
          engine: "pdftotext",
          pages: null,
          text: directText,
        });
        log("✅ JOB COMPLETED (pdftotext)", documentId);
        return;
      }

      // 2) OCR على الصور
      const imgs = await pdfToImages(
        pdf,
        dir,
        Number(maxPages || MAX_PDF_PAGES)
      );

      let fullText = "";
      for (const img of imgs) {
        log(`🔍 Processing page: ${path.basename(img)}`);
        let t = await geminiOCR(img);

        if (!t) {
          log("⚠️ Fallback to Tesseract");
          t = await tesseractOCR(img);
        }

        if (t) fullText += "\n\n" + t;
      }

      if (!fullText.trim()) {
        throw new Error("OCR generated no text");
      }

      await callback(OCR_CALLBACK_URL, {
        ok: true,
        documentId,
        engine: "gemini+tesseract",
        pages: imgs.length,
        text: fullText.trim(),
      });

      log("✅ JOB COMPLETED (OCR)", documentId);
    } finally {
      await fsp.rm(dir, { recursive: true, force: true }).catch(() => {});
    }
  } catch (e) {
    log("❌ JOB FAILED", e.message || e);

    await callback(OCR_CALLBACK_URL, {
      ok: false,
      documentId,
      error: e.message || String(e),
    });
  }
});

// ---------------- Start ----------------
app.listen(PORT, () => {
  log(`🚀 OCR Worker v2.3 running on port ${PORT}`);
});
