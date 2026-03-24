# GENESIS-PRICE-STREAM-EXPORT

**Parquet sidecar to Ingestion Gate — columnar price stream export for GPU consumption**

**Port:** `8794`

> **NVIDIA Phase 2A** — This service is a GPU-readiness pipe. It defines schemas, formats, and CPU-side logic that will activate when NVIDIA RAPIDS / NeMo / Warp hardware arrives. Phase 0 runs pure TypeScript on CPU.

---

## What It Does

1. Ingests price observations from exchange ingestors and the Ingestion Gate in real-time
2. Buffers observations per exchange with configurable maximum buffer size
3. Exports buffered price data to Apache Parquet files (11-column schema) for GPU-native columnar reading
4. Supports auto-flush on timer interval and manual flush (all exchanges or per-exchange)
5. Parses Ingestion Gate format payloads (extracting token, quote, exchange from structured fields)
6. Provides file listing with date and exchange filters for downstream consumers
7. Tracks export statistics: total observations, files written, bytes exported, per-exchange breakdown

---

## Architecture

| File | Purpose | Lines |
|------|---------|-------|
| `src/index.ts` | Express server — 12 endpoints for ingest, flush, streaming, file listing, buffer stats | 147 |
| `src/types.ts` | PriceObservation (11 fields), PriceBatch, ParquetFileInfo, ExportResult, BufferStats, ExporterState | 73 |
| `src/services/price-exporter.service.ts` | Core engine — Parquet schema definition, per-exchange buffering, auto-flush, IG format parsing, file listing | 361 |
| `package.json` | Express + @dsnp/parquetjs dependencies | 18 |
| `Dockerfile` | node:20.20.0-slim, EXPOSE 8794 | 10 |

---

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Service health + state summary |
| GET | `/state` | Full exporter state (buffers, files, auto-flush status) |
| POST | `/ingest` | Ingest a single PriceObservation |
| POST | `/ingest/gate` | Ingest from Ingestion Gate format (auto-parses payload) |
| POST | `/ingest/batch` | Ingest an array of PriceObservations |
| POST | `/flush` | Flush all exchange buffers to Parquet files |
| POST | `/flush/:exchange` | Flush a specific exchange buffer |
| GET | `/files` | List exported Parquet files (with date/exchange filters) |
| GET | `/files/:filename` | Download a specific Parquet file |
| GET | `/buffer/stats` | Per-exchange buffer sizes and totals |
| POST | `/stream/start` | Start auto-flush timer |
| POST | `/stream/stop` | Stop auto-flush timer |

---

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `PORT` | `8794` | HTTP listen port |
| `EXPORT_DIR` | `/app/price-exports` | Directory for Parquet file output |
| `FLUSH_INTERVAL_MS` | `60000` | Auto-flush interval (ms) |
| `MAX_BUFFER_SIZE` | `100000` | Maximum observations per exchange buffer before forced flush |
| `AUTO_FLUSH` | `true` | Enable/disable auto-flush timer on startup |

---

## Integration

- **Reads from:** Ingestion Gate (price observations), Exchange Ingestors (direct price feeds)
- **Writes to:** Parquet files on disk (consumed by RAPIDS cuDF, Morpheus, Warp)
- **Consumed by:** Brighton GPU Interface (spread analysis), Warp Simulation (order book snapshots), any RAPIDS pipeline
- **GPU future:** RAPIDS cuDF reads Parquet natively — zero-copy GPU DataFrames (Phase 2+)

---

## Current State

- **Phase 0 BUILT** — CPU-side TypeScript with @dsnp/parquetjs, fully operational
- 11-column Parquet schema: exchange, pair, token, quote, bidPrice, askPrice, midPrice, spreadBps, volume24h, timestamp, source
- Per-exchange buffering with configurable max size
- Auto-flush timer with manual flush support
- Ingestion Gate format auto-parsing (token/quote extraction)
- File listing with date and exchange filters

---

## Future Editions

1. **RAPIDS cuDF direct** — replace @dsnp/parquetjs with GPU-native Parquet writer via cuDF Python bindings
2. **Streaming Parquet** — append-mode Parquet with row group streaming for continuous GPU consumption
3. **Partitioned storage** — partition by exchange/date/pair for efficient GPU predicate pushdown
4. **S3/MinIO export** — write Parquet to object storage for distributed GPU cluster access
5. **Compression codec** — add Snappy/ZSTD compression to Parquet files for reduced I/O

---

## Rail Deployment

| Rail | Status | Notes |
|------|--------|-------|
| Rail 1 | BUILT | CPU Parquet export, per-exchange buffering, auto-flush, 11-column schema |
| Rail 3 | GPU activation | RAPIDS cuDF native Parquet, streaming append mode |
| Rail 5+ | Full NVIDIA stack | Partitioned storage, S3 export, distributed GPU cluster consumption |
