/* ── GENESIS-PRICE-STREAM-EXPORT — Price Exporter Service ────────── */
/* Buffers price observations from Ingestion Gate, flushes to Parquet  */
/* files on configurable interval. One file per exchange per day.      */
/* RAPIDS cuDF.read_parquet() loads them in milliseconds.              */

import { randomUUID } from "crypto";
import * as path from "path";
import * as fs from "fs";
import { ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";
import {
  PriceObservation,
  PriceBatch,
  ParquetFileInfo,
  ExportResult,
  BufferStats,
  ExporterState,
} from "../types";

/* ── Parquet Schema ──────────────────────────────────────────────── */

const PRICE_SCHEMA = new ParquetSchema({
  exchange:    { type: "UTF8" },
  pair:        { type: "UTF8" },
  token:       { type: "UTF8" },
  quote:       { type: "UTF8" },
  bid:         { type: "DOUBLE" },
  ask:         { type: "DOUBLE" },
  mid:         { type: "DOUBLE" },
  spreadBps:   { type: "DOUBLE" },
  timestamp:   { type: "INT64" },
  hourOfDay:   { type: "INT32" },
  dayOfWeek:   { type: "INT32" },
});

export class PriceExporterService {
  // Buffer: exchange → observation[]
  private buffers: Map<string, PriceObservation[]> = new Map();
  private exportDir: string;
  private flushIntervalMs: number;
  private maxBufferSize: number;
  private flushInterval: ReturnType<typeof setInterval> | null = null;
  private totalIngested = 0;
  private totalExported = 0;
  private totalFilesWritten = 0;
  private allPairs = new Set<string>();
  private allExchanges = new Set<string>();
  private lastFlushAt: string | null = null;
  private lastExportAt: string | null = null;
  private startedAt = Date.now();

  constructor() {
    this.exportDir = process.env.EXPORT_DIR || "/app/price-exports";
    this.flushIntervalMs = parseInt(process.env.FLUSH_INTERVAL_MS || "60000", 10);
    this.maxBufferSize = parseInt(process.env.MAX_BUFFER_SIZE || "100000", 10);

    // Ensure export directory exists
    if (!fs.existsSync(this.exportDir)) {
      fs.mkdirSync(this.exportDir, { recursive: true });
    }
  }

  /* ── Ingest ────────────────────────────────────────────────────── */

  /** Ingest a single price observation */
  ingest(obs: PriceObservation): void {
    const exchange = obs.exchange.toLowerCase();
    if (!this.buffers.has(exchange)) {
      this.buffers.set(exchange, []);
    }
    this.buffers.get(exchange)!.push(obs);
    this.totalIngested++;
    this.allPairs.add(obs.pair);
    this.allExchanges.add(exchange);

    // Force flush if buffer too large
    const buf = this.buffers.get(exchange)!;
    if (buf.length >= this.maxBufferSize) {
      this.flushExchange(exchange);
    }
  }

  /** Ingest from Ingestion Gate format (exchange + ticker results) */
  ingestFromGate(source: string, tickers: Array<{ pair: string; bid: number; ask: number }>): number {
    const now = Date.now();
    const d = new Date(now);
    let count = 0;

    for (const t of tickers) {
      if (!t.pair || isNaN(t.bid) || isNaN(t.ask) || t.bid <= 0 || t.ask <= 0) continue;

      const mid = (t.bid + t.ask) / 2;
      const token = this.extractToken(t.pair);
      const quote = this.extractQuote(t.pair, token);

      this.ingest({
        exchange: source,
        pair: t.pair,
        token,
        quote,
        bid: t.bid,
        ask: t.ask,
        mid: parseFloat(mid.toFixed(8)),
        spreadBps: parseFloat(((t.ask - t.bid) / mid * 10000).toFixed(2)),
        timestamp: now,
        hourOfDay: d.getUTCHours(),
        dayOfWeek: d.getUTCDay(),
      });
      count++;
    }

    return count;
  }

  /** Ingest batch */
  ingestBatch(batch: PriceBatch): number {
    let count = 0;
    for (const obs of batch.observations) {
      this.ingest(obs);
      count++;
    }
    return count;
  }

  /* ── Flush to Parquet ──────────────────────────────────────────── */

  /** Flush all buffers to Parquet files */
  async flushAll(): Promise<ExportResult> {
    const start = Date.now();
    const date = this.today();
    const files: ParquetFileInfo[] = [];
    let totalRows = 0;

    for (const [exchange, buffer] of this.buffers) {
      if (buffer.length === 0) continue;

      const info = await this.writeParquet(exchange, date, buffer);
      if (info) {
        files.push(info);
        totalRows += info.rowCount;
      }
    }

    // Clear all buffers after flush
    this.buffers.clear();
    this.lastFlushAt = new Date().toISOString();
    this.lastExportAt = this.lastFlushAt;

    const result: ExportResult = {
      date,
      filesWritten: files.length,
      totalRows,
      totalSizeBytes: files.reduce((sum, f) => sum + f.sizeBytes, 0),
      files,
      durationMs: Date.now() - start,
    };

    if (files.length > 0) {
      console.log(`[PRICE-EXPORT] Flushed ${totalRows} rows across ${files.length} files (${result.durationMs}ms)`);
    }

    return result;
  }

  /** Flush a single exchange buffer */
  async flushExchange(exchange: string): Promise<ParquetFileInfo | null> {
    const buffer = this.buffers.get(exchange);
    if (!buffer || buffer.length === 0) return null;

    const date = this.today();
    const info = await this.writeParquet(exchange, date, buffer);
    this.buffers.set(exchange, []);
    this.lastFlushAt = new Date().toISOString();

    return info;
  }

  /** Write observations to a Parquet file */
  private async writeParquet(exchange: string, date: string, observations: PriceObservation[]): Promise<ParquetFileInfo | null> {
    if (observations.length === 0) return null;

    const dir = path.join(this.exportDir, exchange);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    // Append to existing file for the day or create new
    const filename = `prices-${exchange}-${date}-${Date.now()}.parquet`;
    const filepath = path.join(dir, filename);

    try {
      const writer = await ParquetWriter.openFile(PRICE_SCHEMA, filepath);

      for (const obs of observations) {
        await writer.appendRow({
          exchange: obs.exchange,
          pair: obs.pair,
          token: obs.token,
          quote: obs.quote,
          bid: obs.bid,
          ask: obs.ask,
          mid: obs.mid,
          spreadBps: obs.spreadBps,
          timestamp: obs.timestamp,
          hourOfDay: obs.hourOfDay,
          dayOfWeek: obs.dayOfWeek,
        });
      }

      await writer.close();

      const stat = fs.statSync(filepath);
      this.totalExported += observations.length;
      this.totalFilesWritten++;

      const info: ParquetFileInfo = {
        filename,
        path: filepath,
        exchange,
        date,
        rowCount: observations.length,
        sizeBytes: stat.size,
        createdAt: new Date().toISOString(),
      };

      return info;
    } catch (err: any) {
      console.error(`[PRICE-EXPORT] Failed to write Parquet for ${exchange}: ${err.message}`);
      return null;
    }
  }

  /* ── Auto-Flush ────────────────────────────────────────────────── */

  startAutoFlush(): void {
    if (this.flushInterval) return;
    console.log(`[PRICE-EXPORT] Auto-flush started (every ${this.flushIntervalMs / 1000}s)`);
    this.flushInterval = setInterval(() => this.flushAll(), this.flushIntervalMs);
  }

  stopAutoFlush(): void {
    if (this.flushInterval) {
      clearInterval(this.flushInterval);
      this.flushInterval = null;
      console.log("[PRICE-EXPORT] Auto-flush stopped");
    }
  }

  /* ── File Listing ──────────────────────────────────────────────── */

  listFiles(exchange?: string, date?: string): ParquetFileInfo[] {
    const files: ParquetFileInfo[] = [];

    const dirs = exchange
      ? [path.join(this.exportDir, exchange)]
      : this.listDirs(this.exportDir);

    for (const dir of dirs) {
      if (!fs.existsSync(dir)) continue;
      const dirName = path.basename(dir);

      for (const filename of fs.readdirSync(dir)) {
        if (!filename.endsWith(".parquet")) continue;
        if (date && !filename.includes(date)) continue;

        const filepath = path.join(dir, filename);
        const stat = fs.statSync(filepath);

        files.push({
          filename,
          path: filepath,
          exchange: dirName,
          date: this.extractDate(filename),
          rowCount: 0, // Would need to read file to get exact count
          sizeBytes: stat.size,
          createdAt: stat.birthtime.toISOString(),
        });
      }
    }

    return files.sort((a, b) => b.createdAt.localeCompare(a.createdAt));
  }

  private listDirs(parent: string): string[] {
    if (!fs.existsSync(parent)) return [];
    return fs.readdirSync(parent)
      .map(d => path.join(parent, d))
      .filter(d => fs.statSync(d).isDirectory());
  }

  /* ── Helpers ───────────────────────────────────────────────────── */

  private extractToken(pair: string): string {
    // BTCUSDT → BTC, ETH-USD → ETH, XRP/USDT → XRP
    const cleaned = pair.replace(/[-_/]/g, "");
    const quotes = ["USDT", "USDC", "BUSD", "USD", "EUR", "GBP", "BTC", "ETH", "DAI", "TUSD"];
    for (const q of quotes) {
      if (cleaned.endsWith(q) && cleaned.length > q.length) {
        return cleaned.slice(0, cleaned.length - q.length);
      }
    }
    return cleaned.slice(0, Math.ceil(cleaned.length / 2));
  }

  private extractQuote(pair: string, token: string): string {
    const cleaned = pair.replace(/[-_/]/g, "");
    if (cleaned.startsWith(token)) {
      return cleaned.slice(token.length) || "USD";
    }
    return "USD";
  }

  private extractDate(filename: string): string {
    const match = filename.match(/(\d{4}-\d{2}-\d{2})/);
    return match ? match[1] : this.today();
  }

  private today(): string {
    return new Date().toISOString().slice(0, 10);
  }

  /* ── State ─────────────────────────────────────────────────────── */

  getBufferStats(): BufferStats[] {
    const stats: BufferStats[] = [];
    for (const [exchange, buffer] of this.buffers) {
      if (buffer.length === 0) continue;
      const pairs = new Set(buffer.map(o => o.pair));
      stats.push({
        exchange,
        observationCount: buffer.length,
        uniquePairs: pairs.size,
        oldestTimestamp: buffer[0].timestamp,
        newestTimestamp: buffer[buffer.length - 1].timestamp,
      });
    }
    return stats;
  }

  getState(): ExporterState {
    let bufferSize = 0;
    for (const [, buffer] of this.buffers) {
      bufferSize += buffer.length;
    }

    return {
      totalObservationsIngested: this.totalIngested,
      totalObservationsExported: this.totalExported,
      totalFilesWritten: this.totalFilesWritten,
      bufferSize,
      buffersByExchange: this.getBufferStats(),
      uniqueExchanges: this.allExchanges.size,
      uniquePairs: this.allPairs.size,
      flushIntervalMs: this.flushIntervalMs,
      exportDir: this.exportDir,
      lastFlushAt: this.lastFlushAt,
      lastExportAt: this.lastExportAt,
      mode: this.flushInterval ? "STREAMING" : "BUFFERED",
      uptime: Date.now() - this.startedAt,
    };
  }
}
