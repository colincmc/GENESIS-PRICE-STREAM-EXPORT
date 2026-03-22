/* ── GENESIS-PRICE-STREAM-EXPORT — Express Server ────────────────── */
/* Ingestion Gate sidecar — captures price ticks to Parquet.           */
/* 66 exchanges × ~500 pairs × 60s intervals = 2M+ rows/day.          */
/* RAPIDS cuDF.read_parquet() loads entire day in milliseconds.        */

import express from "express";
import * as fs from "fs";
import * as path from "path";
import { PriceExporterService } from "./services/price-exporter.service";

const app = express();
app.use(express.json({ limit: "50mb" }));

const PORT = parseInt(process.env.PORT || "8794", 10);
const exporter = new PriceExporterService();

/* ── Health & State ──────────────────────────────────────────────── */

app.get("/health", (_req, res) => {
  const state = exporter.getState();
  res.json({
    service: "genesis-price-stream-export",
    status: "UP",
    mode: state.mode,
    totalIngested: state.totalObservationsIngested,
    totalExported: state.totalObservationsExported,
    bufferSize: state.bufferSize,
    uniqueExchanges: state.uniqueExchanges,
    uniquePairs: state.uniquePairs,
    filesWritten: state.totalFilesWritten,
    uptime: state.uptime,
  });
});

app.get("/state", (_req, res) => {
  res.json(exporter.getState());
});

/* ── Ingest ──────────────────────────────────────────────────────── */

/** Accept raw price observations */
app.post("/ingest", (req, res) => {
  const obs = req.body;
  if (!obs.exchange || !obs.pair || isNaN(obs.bid) || isNaN(obs.ask)) {
    return res.status(400).json({ error: "exchange, pair, bid, ask required" });
  }
  const mid = (obs.bid + obs.ask) / 2;
  const d = new Date();
  exporter.ingest({
    exchange: obs.exchange,
    pair: obs.pair,
    token: obs.token || obs.pair.replace(/USDT|USD|USDC|EUR|GBP$/i, ""),
    quote: obs.quote || "USD",
    bid: obs.bid,
    ask: obs.ask,
    mid: parseFloat(mid.toFixed(8)),
    spreadBps: parseFloat(((obs.ask - obs.bid) / mid * 10000).toFixed(2)),
    timestamp: Date.now(),
    hourOfDay: d.getUTCHours(),
    dayOfWeek: d.getUTCDay(),
  });
  res.json({ accepted: 1 });
});

/** Accept Ingestion Gate format (source + tickers array) */
app.post("/ingest/gate", (req, res) => {
  const { source, tickers } = req.body;
  if (!source || !Array.isArray(tickers)) {
    return res.status(400).json({ error: "source and tickers[] required" });
  }
  const count = exporter.ingestFromGate(source, tickers);
  res.json({ accepted: count, source });
});

/** Accept batch of observations */
app.post("/ingest/batch", (req, res) => {
  const batch = req.body;
  if (!Array.isArray(batch.observations)) {
    return res.status(400).json({ error: "observations[] required" });
  }
  const count = exporter.ingestBatch(batch);
  res.json({ accepted: count });
});

/* ── Flush & Export ──────────────────────────────────────────────── */

app.post("/flush", async (_req, res) => {
  const result = await exporter.flushAll();
  res.json(result);
});

app.post("/flush/:exchange", async (req, res) => {
  const info = await exporter.flushExchange(req.params.exchange);
  if (!info) return res.json({ flushed: 0, exchange: req.params.exchange });
  res.json(info);
});

/* ── Streaming Control ───────────────────────────────────────────── */

app.post("/stream/start", (_req, res) => {
  exporter.startAutoFlush();
  res.json({ streaming: true, interval: exporter.getState().flushIntervalMs });
});

app.post("/stream/stop", (_req, res) => {
  exporter.stopAutoFlush();
  res.json({ streaming: false });
});

/* ── File Listing & Download ─────────────────────────────────────── */

app.get("/files", (req, res) => {
  const exchange = req.query.exchange as string | undefined;
  const date = req.query.date as string | undefined;
  const files = exporter.listFiles(exchange, date);
  res.json({ count: files.length, files });
});

app.get("/files/:exchange/:filename", (req, res) => {
  const exportDir = exporter.getState().exportDir;
  const filepath = path.join(exportDir, req.params.exchange, req.params.filename);
  if (!fs.existsSync(filepath)) return res.status(404).json({ error: "File not found" });
  res.download(filepath);
});

/* ── Buffer Stats ────────────────────────────────────────────────── */

app.get("/buffers", (_req, res) => {
  res.json(exporter.getBufferStats());
});

/* ── Start ───────────────────────────────────────────────────────── */

// Auto-start streaming if configured
const autoStart = process.env.AUTO_FLUSH === "true";
if (autoStart) {
  exporter.startAutoFlush();
}

app.listen(PORT, () => {
  const state = exporter.getState();
  console.log(`[PRICE-EXPORT] Listening on port ${PORT}`);
  console.log(`[PRICE-EXPORT] Export dir: ${state.exportDir}`);
  console.log(`[PRICE-EXPORT] Flush interval: ${state.flushIntervalMs / 1000}s`);
  console.log(`[PRICE-EXPORT] Auto-flush: ${autoStart ? "ENABLED" : "disabled (POST /stream/start to enable)"}`);
});
