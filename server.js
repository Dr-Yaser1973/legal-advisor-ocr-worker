 import express from "express";
import fs from "fs";
import path from "path";
import { execSync } from "child_process";
import Tesseract from "tesseract.js";

const PORT = process.env.PORT || 10000;

const app = express();
app.use(express.json({ limit: "50mb" }));

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

// ==========================
// Health
// ==========================
app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

// ==========================
// OCR endpoint
// ==========================
app.post("/ocr", async (req, res) => {
  try {
    const { filePath } = req.body;

    if (!filePath || !fs.existsSync(filePath)) {
      return res.status(400).json({ ok: false, error: "filePath not found" });
    }

    const workDir = `/tmp/ocr-${Date.now()}`;
    fs.mkdirSync(workDir, { recursive: true });

    // PDF → images (Linux native)
    execSync(
      `pdftoppm -png "${filePath}" "${path.join(workDir, "page")}"`,
      { stdio: "ignore" }
    );

    const images = fs
      .readdirSync(workDir)
      .filter(f => f.endsWith(".png"))
      .map(f => path.join(workDir, f));

    let text = "";

    for (const img of images) {
      const r = await Tesseract.recognize(img, "ara+eng", {
        tessedit_pageseg_mode: 6
      });
      text += "\n" + r.data.text;
    }

    fs.rmSync(workDir, { recursive: true, force: true });

    res.json({
      ok: true,
      pages: images.length,
      text: text.trim()
    });

  } catch (e) {
    log("OCR ERROR", e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ==========================
// Start
// ==========================
app.listen(PORT, "0.0.0.0", () => {
  log("OCR SERVICE RUNNING ON", PORT);
});
