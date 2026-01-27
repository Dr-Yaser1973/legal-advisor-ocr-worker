// server.js
import express from "express";
import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";
import { GoogleGenerativeAI } from "@google/generative-ai";

const app = express();
app.use(express.json({ limit: "20mb" }));

// ===============================
// ENV
// ===============================
const PORT = process.env.PORT || 10000;
const WORKER_SECRET = process.env.OCR_WORKER_SECRET;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL = process.env.GEMINI_MODEL || "gemini-2.0-flash";

// ===============================
// Clients
// ===============================
const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
);

const genAI = new GoogleGenerativeAI(GEMINI_API_KEY);
const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });

// ===============================
// Utils
// ===============================
function requireSecret(req, res) {
  const secret = req.headers["x-worker-secret"];
  if (!WORKER_SECRET || secret !== WORKER_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }
}

async function downloadFromSupabase(bucket, path) {
  const { data, error } = await supabase.storage
    .from(bucket)
    .download(path);

  if (error || !data) {
    throw new Error("File not found in Supabase");
  }

  const buffer = Buffer.from(await data.arrayBuffer());
  return buffer;
}

// ===============================
// ROUTES
// ===============================
app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "ocr-worker" });
});

// 🔥 هذا هو المسار الذي كان ناقص
app.post("/run", async (req, res) => {
  try {
    // حماية بالسيكرت
    const unauthorized = requireSecret(req, res);
    if (unauthorized) return;

    const { bucket, path, mimetype, language } = req.body || {};

    if (!bucket || !path) {
      return res.status(400).json({ error: "bucket and path are required" });
    }

    console.log("OCR request:", { bucket, path, mimetype, language });

    // ===============================
    // Download file
    // ===============================
    const fileBuffer = await downloadFromSupabase(bucket, path);

    // ===============================
    // Gemini OCR
    // ===============================
    const prompt = `
You are a professional OCR system.
Extract ALL readable text from this document.
Preserve structure, headings, and paragraphs.
Language preference: ${language || "ar+en"}
`;

    const result = await model.generateContent([
      {
        inlineData: {
          data: fileBuffer.toString("base64"),
          mimeType: mimetype || "application/pdf",
        },
      },
      prompt,
    ]);

    const text =
      result?.response?.candidates?.[0]?.content?.parts?.[0]?.text ||
      "";

    if (!text) {
      throw new Error("Gemini returned empty text");
    }

    return res.json({
      ok: true,
      text,
      pages: null,
      engine: "gemini",
    });
  } catch (err) {
    console.error("OCR RUN ERROR:", err);
    return res.status(500).json({
      error: err.message || "OCR failed",
    });
  }
});

// ===============================
app.listen(PORT, () => {
  console.log("OCR Worker running on port", PORT);
});
