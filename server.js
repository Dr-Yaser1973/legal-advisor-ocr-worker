 import express from "express";
import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

const app = express();
app.use(express.json({ limit: "20mb" }));

const PORT = process.env.PORT || 10000;

// ================== Security ==================
function assertAuth(req) {
  const secret = process.env.OCR_WORKER_SECRET;
  if (!secret) return; // dev mode

  const token = req.headers["x-worker-secret"];
  if (token !== secret) {
    throw new Error("Unauthorized");
  }
}

// ================== Supabase ==================
function supabaseAdmin() {
  return createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_ROLE_KEY,
    { auth: { persistSession: false } }
  );
}

// ================== Gemini OCR ==================
async function runGeminiOCR(buffer, mime, lang) {
  const apiKey = process.env.GEMINI_API_KEY;
  const model = process.env.GEMINI_MODEL || "gemini-2.0-flash";

  const base64 = Buffer.from(buffer).toString("base64");

  const res = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [
          {
            role: "user",
            parts: [
              {
                text: `استخرج النص الكامل من هذا المستند بدقة عالية.
اللغة المتوقعة: ${lang || "ar+en"}.
أعد النص فقط بدون شرح.`,
              },
              {
                inlineData: {
                  mimeType: mime || "application/pdf",
                  data: base64,
                },
              },
            ],
          },
        ],
      }),
    }
  );

  const json = await res.json();
  const text =
    json?.candidates?.[0]?.content?.parts
      ?.map((p) => p.text || "")
      .join("\n") || "";

  if (!text.trim()) {
    throw new Error("Gemini returned empty OCR text");
  }

  return text;
}

// ================== OCR Route ==================
app.post("/run", async (req, res) => {
  try {
    assertAuth(req);

    const { bucket, path, mimetype, language } = req.body;

    if (!bucket || !path) {
      return res.status(400).json({ error: "bucket and path required" });
    }

    console.log("OCR RUN:", { bucket, path });

    const sb = supabaseAdmin();
    const { data, error } = await sb.storage.from(bucket).download(path);

    if (error || !data) {
      return res.status(404).json({ error: "File not found in Supabase" });
    }

    const buffer = Buffer.from(await data.arrayBuffer());
    const text = await runGeminiOCR(buffer, mimetype, language);

    return res.json({
      ok: true,
      length: text.length,
      text,
    });
  } catch (err) {
    console.error("OCR ERROR:", err.message);
    return res.status(401).json({ error: err.message });
  }
});

// ================== Health ==================
app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "legal-advisor-ocr-worker" });
});

app.listen(PORT, () => {
  console.log("OCR Worker running on port", PORT);
});

