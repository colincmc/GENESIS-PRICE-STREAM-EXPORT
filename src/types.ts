/* ── GENESIS-PRICE-STREAM-EXPORT — Types ─────────────────────────── */
/* Columnar export of price stream observations from Ingestion Gate.   */
/* 100K+ price ticks → Parquet files → RAPIDS cuDF loads in ms.       */

/** A single price observation from any exchange */
export interface PriceObservation {
  exchange: string;           // "binance", "kraken", etc.
  pair: string;               // "BTCUSDT", "ETHGBP", etc.
  token: string;              // Base token: "BTC", "ETH"
  quote: string;              // Quote currency: "USDT", "GBP"
  bid: number;
  ask: number;
  mid: number;                // (bid + ask) / 2
  spreadBps: number;          // (ask - bid) / mid * 10000
  timestamp: number;          // Unix ms
  hourOfDay: number;          // 0-23 UTC
  dayOfWeek: number;          // 0-6 (Sun=0)
}

/** Batch of observations for bulk ingest */
export interface PriceBatch {
  source: string;
  observations: PriceObservation[];
  receivedAt: string;
}

/** Parquet file metadata */
export interface ParquetFileInfo {
  filename: string;
  path: string;
  exchange: string;           // "all" or specific exchange
  date: string;               // YYYY-MM-DD
  rowCount: number;
  sizeBytes: number;
  createdAt: string;
}

/** Export result */
export interface ExportResult {
  date: string;
  filesWritten: number;
  totalRows: number;
  totalSizeBytes: number;
  files: ParquetFileInfo[];
  durationMs: number;
}

/** Buffer statistics */
export interface BufferStats {
  exchange: string;
  observationCount: number;
  uniquePairs: number;
  oldestTimestamp: number;
  newestTimestamp: number;
}

/** Service state */
export interface ExporterState {
  totalObservationsIngested: number;
  totalObservationsExported: number;
  totalFilesWritten: number;
  bufferSize: number;
  buffersByExchange: BufferStats[];
  uniqueExchanges: number;
  uniquePairs: number;
  flushIntervalMs: number;
  exportDir: string;
  lastFlushAt: string | null;
  lastExportAt: string | null;
  mode: "BUFFERED" | "STREAMING";
  uptime: number;
}
