import express from "express";
import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";
import Tesseract from "tesseract.js";
import { GoogleGenerativeAI } from "@google/generative-ai";

// =======================
// ENV
// =======================
const PORT = process.env.PORT || 10000;
const WORKER_SECRET = (process.env.OCR_WORKER_SECRET || "").trim();
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const ENGINE = process.env.OCR_ENGINE || "hybrid";

// =======================
// Init
// =======================
const app = express();
app.use(express.json({ limit: "10mb" }));

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const genAI = GEMINI_API_KEY
  ? new GoogleGenerativeAI(GEMINI_API_KEY)
  : null;

// =======================
// Helpers
// =======================
function getHeader(req, name) {
  return (
    req.headers[name] ||
    req.headers[name.toLowerCase()] ||
    req.get(name)
  );
}

function unauthorized(res, msg) {
  console.log("UNAUTHORIZED:", msg);
  return res.status(401).json({ ok: false, error: "Unauthorized" });
}

// =======================
// Health
// =======================
app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "ocr-worker",
    secretLoaded: Boolean(WORKER_SECRET),
    engine: ENGINE,
  });
});

// =======================
// Auth Middleware (Worker Only)
// =======================
app.use((req, res, next) => {
  if (req.path === "/health") return next();

  const incoming = (getHeader(req, "x-worker-secret") || "").trim();

  console.log("HEADER RECEIVED:", incoming || "EMPTY");
  console.log("SECRET LOADED:", WORKER_SECRET || "EMPTY");

  if (!incoming || !WORKER_SECRET) {
    return unauthorized(res, "missing");
  }

  if (incoming !== WORKER_SECRET) {
    return unauthorized(res, "mismatch");
  }

  next();
});

// =======================
// OCR Logic
// =======================
async function runTesseract(buffer, lang) {
  const result = await Tesseract.recognize(buffer, lang || "ara+eng");
  return result.data.text || "";
}

async function runGemini(text) {
  if (!genAI) return text;

  const model = genAI.getGenerativeModel({ model: "gemini-2.0-flash" });
  const prompt = `
أعد صياغة النص التالي كنص قانوني منسق وواضح بدون حذف أي معلومات:

${text}
  `;
  const res = await model.generateContent(prompt);
  return res.response.text();
}

// =======================
// Run Endpoint
// =======================
app.post("/run", async (req, res) => {
  try {
    const { bucket, path, mimetype, language } = req.body || {};

    if (!bucket || !path) {
      return res.status(400).json({ ok: false, error: "bucket/path required" });
    }

    // 1) Signed URL
    const { data, error } = await supabase.storage
      .from(bucket)
      .createSignedUrl(path, 300);

    if (error || !data?.signedUrl) {
      return res.status(404).json({
        ok: false,
        error: "File not found in Supabase",
      });
    }

    // 2) Download file
    const fileRes = await fetch(data.signedUrl);
    const buffer = Buffer.from(await fileRes.arrayBuffer());

    // 3) OCR
    let rawText = "";
    if (ENGINE === "tesseract" || ENGINE === "hybrid") {
      rawText = await runTesseract(buffer, language || "ara+eng");
    }

    // 4) Gemini polish
    let finalText = rawText;
    if (ENGINE === "gemini" || ENGINE === "hybrid") {
      finalText = await runGemini(rawText);
    }

    return res.json({
      ok: true,
      engine: ENGINE,
      pages: null,
      text: finalText,
    });
  } catch (e) {
    console.error("OCR RUN ERROR:", e);
    return res.status(500).json({
      ok: false,
      error: e.message || "OCR failed",
    });
  }
});

// =======================
// Start
// =======================
app.listen(PORT, () => {
  console.log("SECRET LOADED:", Boolean(WORKER_SECRET));
  console.log(`OCR Worker running on port ${PORT}`);
});

