console.log("SERVER VERSION: OCR-RUN-ENABLED");
import express from "express";
import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";
import { GoogleGenerativeAI } from "@google/generative-ai";
import Tesseract from "tesseract.js";

const app = express();
app.use(express.json({ limit: "10mb" }));

// ======================
// ENV
// ======================
const PORT = process.env.PORT || 10000;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const WORKER_SECRET = process.env.OCR_WORKER_SECRET;

// ======================
console.log("SECRET LOADED:", WORKER_SECRET ? "YES" : "NO");

// ======================
// Clients
// ======================
const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
);

const genAI = GEMINI_API_KEY
  ? new GoogleGenerativeAI(GEMINI_API_KEY)
  : null;

// ======================
// Helpers
// ======================
function unauthorized(res, msg = "Unauthorized") {
  return res.status(401).json({ ok: false, error: msg });
}

// ======================
// Routes
// ======================
app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "ocr-worker" });
});

// ======================
// MAIN OCR ENDPOINT
// ======================
app.post("/run", async (req, res) => {
  try {
    const incomingSecret = req.headers["x-worker-secret"];

    console.log("HEADER RECEIVED:", incomingSecret || "EMPTY");
    console.log("SECRET LOADED:", WORKER_SECRET || "MISSING");

    if (!incomingSecret) {
      console.log("UNAUTHORIZED: missing header");
      return unauthorized(res, "Missing worker secret");
    }

    if (incomingSecret !== WORKER_SECRET) {
      console.log("UNAUTHORIZED: invalid secret");
      return unauthorized(res, "Invalid worker secret");
    }

    const { bucket, path, mimetype, language } = req.body || {};

    if (!bucket || !path) {
      return res.status(400).json({
        ok: false,
        error: "bucket and path are required",
      });
    }

    console.log("OCR REQUEST:", { bucket, path, mimetype, language });

    // ======================
    // Download file from Supabase
    // ======================
    const { data, error } = await supabase.storage
      .from(bucket)
      .download(path);

    if (error || !data) {
      console.log("SUPABASE DOWNLOAD ERROR:", error);
      return res.status(404).json({
        ok: false,
        error: "File not found in Supabase",
      });
    }

    const buffer = Buffer.from(await data.arrayBuffer());

    let extractedText = "";

    // ======================
    // OCR ENGINE
    // ======================
    if (genAI) {
      console.log("Using Gemini OCR");
      const model = genAI.getGenerativeModel({
        model: "gemini-2.0-flash",
      });

      const prompt = `
Extract all readable text from this document.
Language preference: ${language || "ar+en"}
Return plain text only.
`;

      const result = await model.generateContent([
        {
          inlineData: {
            data: buffer.toString("base64"),
            mimeType: mimetype || "application/pdf",
          },
        },
        prompt,
      ]);

      extractedText = result.response.text();
    } else {
      console.log("Using Tesseract OCR");
      const result = await Tesseract.recognize(buffer, "ara+eng");
      extractedText = result.data.text;
    }

    // ======================
    // DONE
    // ======================
    return res.json({
      ok: true,
      engine: genAI ? "gemini" : "tesseract",
      text: extractedText,
      length: extractedText.length,
    });
  } catch (err) {
    console.error("OCR WORKER ERROR:", err);
    return res.status(500).json({
      ok: false,
      error: err?.message || "OCR failed",
    });
  }
});

// ======================
app.listen(PORT, () => {
  console.log("OCR Worker running on port", PORT);
});
console.log("ROUTES REGISTERED: /health, /run");
