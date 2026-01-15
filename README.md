# KeplerDB

> **Status:** MVP completed (v0.1)  
> Currently undergoing a large-scale refactor for ergonomics, testing, and performance evaluation.

<div align="center">
  <img width="800" height="525" alt="ChatGPT Image Dec 24, 2025, 03_39_55 AM" src="https://github.com/user-attachments/assets/48ec9b43-90f4-44e4-9ecf-4e9029c5573f" />
</div>
<p align="center">
    <img src="https://img.shields.io/badge/version-v0.1.0-green" />
    <img src="https://img.shields.io/badge/MSRV-v1.85.1-blue" />
    <img src="https://img.shields.io/badge/dependencies-0%20of%205%20outdated-green" />
    <img src="https://img.shields.io/badge/documents-unavailable-red" />
</p>


KeplerDB is a lightweight, high-performance **embedded key-value store** written in Rust.  
It implements the **core building blocks of modern LSM-tree storage engines**, focusing on correctness, crash safety, and clear internal architecture rather than feature completeness.

## Features

- **Write-Ahead Log (WAL)** for durability and crash recovery  
- **MemTable (BTreeMap-based)** for fast in-memory writes  
- **Immutable MemTables (ImmTables)** with background flushing  
- **SSTables** with:
  - Sparse index
  - Bloom filter
- **Manifest log** for recoverable metadata persistence  
- **Threaded architecture**
  - WAL writer (Journal)
  - SST writer
  - Manifest writer
- **Crash recovery**
  - WAL replay
  - Manifest-based SST restoration
- **Minimal dependencies**
- **Zero external storage engines**

---
## How to use
```rust
// start new DB  
let db = Kepler::new("aa")?;

// add some data
db.insert(b"hello", b"good")?;

// retrieve data from DB
let found_val: Option<Bytes> = db.get(b"hello")?;

// remove data
db.remove(b"hello")?;

```
---

## Performance

Kepler demonstrates strong performance in write-heavy and sequential workloads, achieving **1.7×–4.8× lower latency** compared to sled and redb in controlled single-threaded benchmarks.
Benchmarks were executed with CPU affinity pinning and `rdtscp`-based cycle measurements to minimize scheduler noise and timing skew.

### Random Write (µs/op)

<img width="640" height="480" alt="rand_write" src="https://github.com/user-attachments/assets/ad779029-ae1c-403a-9524-3250a1aadfa5" />

### Highlights

- 🚀 **Random Write:** Up to **4.8× faster** than redb and **3.5× faster** than sled  
- ⚡ **Sequential Write:** ~**2.3× faster** than sled  
- 📈 **Sequential Read:** Consistently faster than sled and competitive with redb  
- 🔬 **Random Read:** Currently slower than redb, indicating clear optimization opportunities  

Full benchmark methodology, raw results, and additional plots are available in [`benches/`](./benches).

---

## Documentation (TODO)

- [ ] Public API doc comments (`///`)
- [ ] Module-level documentation
- [ ] Example usage

---

## Planned Features

- [ ] **Compaction** (SST merging)
- [ ] **CLI interface** for interaction
- [ ] **Batch write support**
- [ ] More sophisticated **error handling patterns**
- [ ] Value format optimizations
- [ ] SST indexing for range queries

---

## Module Breakdown

| File | Description |
|------|-------------|
| `lib.rs` | Crate entry point and public exports |
| `db.rs` | Public database API (`Kepler`) and user-facing interface |
| `journal.rs` | Write-Ahead Log (WAL) implementation and recovery logic |
| `mem_table.rs` | In-memory MemTable with seqno tracking |
| `imm_tables.rs` | Immutable MemTable queue for background flushing |
| `table_set.rs` | Orchestration layer combining MemTable, ImmTables, and SSTables |
| `sst_writer.rs` | SSTable writer and flush logic |
| `sstable.rs` | SSTable reader, sparse index, and bloom filter lookup |
| `sst_manager.rs` | SSTable set management and lookup coordination |
| `manifest.rs` | Manifest log for persistent SST metadata |
| `version.rs` | Versioned SST state and recovery metadata |
| `bloom.rs` | Bloom filter implementation |
| `error.rs` | Unified error type and escalation semantics |
| `traits.rs` | Core engine traits (`Getable`, `Putable`) |
| `types.rs` | Shared engine types and worker signals |
| `constants.rs` | File format constants and layout definitions |
| `utils.rs` | Low-level helpers (fs helpers, encoding, parsing utilities) |



---

## Planned Work (Post-MVP)
- [ ] Compaction (SST merging and level management)
- [ ] Batch write support
- [ ] CLI interface
- [ ] Improved error classification and recovery policies
- [ ] Value encoding and format optimizations
- [ ] Range queries and iterator support

---


