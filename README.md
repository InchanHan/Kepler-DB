# KeplerDB 🪐 (MVP completed, now in large refactory)

> **Status:** MVP completed (v0.1)  
> Currently undergoing a large-scale refactor for ergonomics, testing, and performance evaluation.

<div align="center">
  <img width="800" height="525" alt="ChatGPT Image Dec 24, 2025, 03_39_55 AM" src="https://github.com/user-attachments/assets/48ec9b43-90f4-44e4-9ecf-4e9029c5573f" />
</div>

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
  - WAL writer
  - Flush worker
  - Manifest writer
- **Crash recovery**
  - WAL replay
  - Manifest-based SST restoration
- **Minimal dependencies**
- **Zero external storage engines**

---
## How to use
```rust
fn main() -> Result<()> {
  // start new DB  
  let db = Kepler::new("aa")?;

  // add some data
  db.insert(b"hello", b"good")?;

  // retrieve data from DB
  let founded_val: Option<Bytes> = db.get(b"hello")?

  // remove data
  db.remove(b"hello")?;

  Ok(())
}
```
---

## 🧪 Testing (TODO)

- [ ] Unit tests for `MemTable`, `WAL`, and `Manifest`
- [ ] Integration tests with crash recovery scenarios
- [ ] Performance benchmarks for write/read throughput

---

## 📚 Documentation (TODO)

- [ ] Public API doc comments (`///`)
- [ ] Module-level documentation
- [ ] Example usage

---

## 🔧 Planned Features

- [ ] **Compaction** (SST merging)
- [ ] **CLI interface** for interaction
- [ ] **Batch write support**
- [ ] More sophisticated **error handling patterns**
- [ ] Value format optimizations
- [ ] SST indexing for range queries

---

## 📂 Current Module Breakdown

| Module         | Description                                  |
|----------------|----------------------------------------------|
| `db.rs`        | Entry point of KeplerDB (API interface)      |
| `memtable.rs`  | In-memory BTreeMap with seqno tracking       |
| `wal_writer.rs`| WAL write logic + append/fsync               |
| `flush_worker.rs` | SST writer + flush thread                 |
| `recovery.rs`  | Recovery logic from WAL + manifest           |
| `manifest.rs`  | Metadata append log for SSTs                 |
| `value.rs`     | Value enum (Data, Tombstone) abstraction     |
| `error.rs`     | Custom error type `KeplerErr`                |
| `constants.rs` | Constants for file paths, formats, etc.      |
| `utils.rs`     | Helper utilities and abstractions            |

---

## 🚀 Status

> ✅ MVP complete  
> 🔧 Currently doing large refactory  
> 🧪 Planning performance evaluation and benchmarks  
> 🧠 Designed for learning and future extension into full DB/FS engines

---

## 📝 License

MIT

