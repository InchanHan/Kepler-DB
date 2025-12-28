# KeplerDB 🪐 (MVP completed, now in large refactory)
KeplerDB is a lightweight, high-performance embedded key-value store written in Rust.  
It implements core components of modern LSM-tree-based databases, including WAL (Write-Ahead Logging), MemTables, background flush workers, and manifest logging for crash-safe recovery.
<div align="center">
  <img width="800" height="525" alt="ChatGPT Image Dec 24, 2025, 03_39_55 AM" src="https://github.com/user-attachments/assets/48ec9b43-90f4-44e4-9ecf-4e9029c5573f" />
</div>

- **Write-Ahead Log (WAL)** for durability
- **MemTable (BTreeMap)** for fast in-memory writes
- **Flush Worker** for async flushing to on-disk SST files
- **Manifest Logging** for recoverable metadata persistence
- **Threaded Architecture** (WAL writer, flush worker, manifest writer)
- **Crash Recovery** from WAL and Manifest
- **Minimal Dependencies**, Zero External Storage Engines

---
## How to use
```rust
fn main() -> KeplerResult<()> {
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

## 📐 Architecture

<img width="535" height="537" alt="Screenshot 2025-12-24 at 1 32 11 AM" src="https://github.com/user-attachments/assets/3ce44692-3a4b-4fee-bc9e-23754b412ae2" />

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

