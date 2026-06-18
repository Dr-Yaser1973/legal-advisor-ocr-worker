 // legal-advisor-ocr-worker/server.js
import express from "express";
import { createClient } from "@supabase/supabase-js";
import { GoogleGenAI, createPartFromUri } from "@google/genai";
import fs from "fs";
import fsp from "fs/promises";
import path from "path";
import os from "os";
import crypto from "crypto";

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
const GEMINI_MODEL = process.env.GEMINI_MODEL || "gemini-flash-latest";

// الحد الذي نتجاوز فوقه إلى Files API بدل inline. ~15MB احتياطاً.
const INLINE_LIMIT_BYTES = Number(
  process.env.INLINE_LIMIT_BYTES || String(15 * 1024 * 1024)
);

const CALLBACK_TIMEOUT_MS = Number(process.env.CALLBACK_TIMEOUT_MS || "15000");
const MAX_CALLBACK_TEXT_CHARS = Number(
  process.env.MAX_CALLBACK_TEXT_CHARS || "200000"
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

const ai = new GoogleGenAI({ apiKey: GEMINI_API_KEY });

// ---------------- Utils ----------------
function log(...a) {
  console.log(new Date().toISOString(), ...a);
}

function tmpDir() {
  const d = path.join(os.tmpdir(), "ocr-" + crypto.randomBytes(8).toString("hex"));
  fs.mkdirSync(d, { recursive: true });
  return d;
}

// موجّه استخراج النص — مضبوط للمستندات القانونية العربية
const OCR_PROMPT = `أنت أداة استخراج نص دقيقة (OCR). استخرج النص الكامل من هذا المستند حرفياً.
قواعد صارمة:
- أعِد النص كما هو تماماً دون تلخيص أو تفسير أو إضافة أو حذف.
- حافظ على ترتيب الفقرات والأسطر والترقيم القانوني (المواد، البنود، الفقرات).
- لا تترجم. أبقِ النص بلغته الأصلية (عربي و/أو إنجليزي).
- إن كان هناك جداول، فرّغها كنص منظّم مع الحفاظ على العلاقات بين الأعمدة.
- لا تكتب أي مقدمة أو تعليق أو خاتمة. أعِد النص المستخرج فقط.`;

// ---------------- Download from Supabase ----------------
async function download(bucket, objectPath, out) {
  log("📥 Download", bucket, objectPath);
  const { data, error } = await supabase.storage
    .from(bucket)
    .download(objectPath);
  if (error) throw new Error(error.message);
  const buf = Buffer.from(await data.arrayBuffer());
  await fsp.writeFile(out, buf);
  return buf.length;
}

// ---------------- Gemini OCR ----------------
async function geminiOcrInline(pdfBuffer) {
  const response = await ai.models.generateContent({
    model: GEMINI_MODEL,
    contents: [
      { text: OCR_PROMPT },
      {
        inlineData: {
          mimeType: "application/pdf",
          data: pdfBuffer.toString("base64"),
        },
      },
    ],
  });
  return response.text;
}

async function geminiOcrViaFileApi(pdfPath) {
  log("⬆️ Uploading to Gemini Files API (large file)");

  const fileBlob = new Blob([fs.readFileSync(pdfPath)], {
    type: "application/pdf",
  });

  const uploaded = await ai.files.upload({
    file: fileBlob,
    config: { displayName: "ocr-doc.pdf" },
  });

  // انتظار معالجة الملف حتى ACTIVE
  let getFile = await ai.files.get({ name: uploaded.name });
  let waited = 0;
  while (getFile.state === "PROCESSING" && waited < 60000) {
    await new Promise((r) => setTimeout(r, 3000));
    waited += 3000;
    getFile = await ai.files.get({ name: uploaded.name });
  }
  if (getFile.state === "FAILED") {
    throw new Error("Gemini file processing failed");
  }
  if (getFile.state !== "ACTIVE") {
    throw new Error(`Gemini file not active: ${getFile.state}`);
  }

  const contents = [{ text: OCR_PROMPT }];
  if (getFile.uri && getFile.mimeType) {
    contents.push(createPartFromUri(getFile.uri, getFile.mimeType));
  }

  const response = await ai.models.generateContent({
    model: GEMINI_MODEL,
    contents,
  });

  // تنظيف الملف من Gemini بعد الاستخدام (best-effort)
  ai.files.delete({ name: uploaded.name }).catch(() => {});

  return response.text;
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
    engine: "gemini",
    model: GEMINI_MODEL,
  });
});

app.post("/ocr", async (req, res) => {
  const { documentId, bucket, objectPath } = req.body;

  if (req.headers["x-worker-secret"] !== OCR_WORKER_SECRET) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }

  if (!documentId || !bucket || !objectPath) {
    return res.status(400).json({ ok: false, error: "Missing fields" });
  }

  log("📄 JOB RECEIVED", { documentId, objectPath });

  // رد فوري — المعالجة في الخلفية
  res.json({ ok: true, message: "Processing started" });

  const dir = tmpDir();
  const pdf = path.join(dir, "input.pdf");

  try {
    const size = await download(bucket, objectPath, pdf);
    log("📦 File size", { bytes: size });

    let text = "";
    if (size <= INLINE_LIMIT_BYTES) {
      log("🧠 Gemini OCR (inline)", { model: GEMINI_MODEL });
      const buf = await fsp.readFile(pdf);
      text = await geminiOcrInline(buf);
    } else {
      log("🧠 Gemini OCR (Files API)", { model: GEMINI_MODEL });
      text = await geminiOcrViaFileApi(pdf);
    }

    text = (text || "").replace(/\0/g, "").trim();

    if (!text) {
      await callback(OCR_CALLBACK_URL, {
        ok: false,
        documentId,
        error: "Gemini returned no text",
      });
      log("❌ JOB FAILED (no text)", documentId);
      return;
    }

    await callback(OCR_CALLBACK_URL, {
      ok: true,
      documentId,
      engine: "gemini",
      model: GEMINI_MODEL,
      text,
    });

    log("✅ JOB COMPLETED", { documentId, chars: text.length });
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
  log(`🚀 OCR Worker (Gemini) running on port ${PORT}`);
});