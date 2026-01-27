// server.js 
import express from "express";
import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";
import { GoogleGenerativeAI } from "@google/generative-ai";
import Tesseract from "tesseract.js";

const app = express();
app.use(express.json({ limit: "20mb" }));

const PORT = process.env.PORT || 10000;
const SECRET = process.env.OCR_WORKER_SECRET;

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

// ========================
// Health
// ========================
app.get("/health", (req, res) => {
  res.json({ ok: true, service: "ocr-worker" });
});

// ========================
// Auth Middleware
// ========================
function requireSecret(req, res, next) {
  const secret = req.headers["x-worker-secret"];
  if (!SECRET || secret !== SECRET) {
    console.log("❌ Unauthorized request");
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
}

// ========================
// OCR RUN
// ========================
app.post("/run", requireSecret, async (req, res) => {
  try {
    const { bucket, path, mimetype, language } = req.body;

    if (!bucket || !path) {
      return res.status(400).json({ error: "bucket and path required" });
    }

    console.log("📄 OCR REQUEST:", { bucket, path, mimetype, language });

    // ========================
    // Download from Supabase
    // ========================
    const { data, error } = await supabase.storage
      .from(bucket)
      .download(path);

    if (error || !data) {
      console.error("❌ Supabase download error", error);
      return res.status(404).json({ error: "File not found in storage" });
    }

    const buffer = Buffer.from(await data.arrayBuffer());

    let text = "";

    // ========================
    // Gemini First (Fast)
    // ========================
    try {
      const model = genAI.getGenerativeModel({ model: "gemini-1.5-flash" });

      const prompt = `
استخرج النص الكامل من هذا المستند PDF بدقة عالية.
اللغة المطلوبة: ${language || "ar+en"}
رجاءً أعد النص فقط بدون أي شرح.
`;

      const result = await model.generateContent([
        { text: prompt },
        {
          inlineData: {
            mimeType: mimetype || "application/pdf",
            data: buffer.toString("base64"),
          },
        },
      ]);

      text = result.response.text();
      console.log("⚡ Gemini OCR success");
    } catch (geminiErr) {
      console.log("⚠️ Gemini failed, fallback to Tesseract");

      // ========================
      // Tesseract Fallback
      // ========================
      const ocr = await Tesseract.recognize(buffer, "ara+eng");
      text = ocr.data.text;
    }

    return res.json({ ok: true, text });
  } catch (err) {
    console.error("🔥 OCR FAILED", err);
    return res.status(500).json({ error: "OCR processing failed" });
  }
});
console.log("SECRET LOADED:", SECRET ? "YES" : "NO");

// ========================
app.listen(PORT, () => {
  console.log(`OCR Worker running on port ${PORT}`);
});
