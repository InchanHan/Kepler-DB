# Kepler Benchmark Report for v.0.1.0

This document presents a controlled microbenchmark comparing the single-threaded hot-path performance of **Kepler**, **sled**, and **redb** under identical workloads.

The primary goal is to evaluate raw read/write latency characteristics while minimizing operating system noise and timing skew.

---

## Overview

- Workload: Sequential and random read/write operations
- Databases compared:
  - Kepler (custom LSM-based engine)
  - sled
  - redb (used batch)
- Metric: Median latency per operation (µs/op)
- Benchmark harness: Chronix (`#[chronixer]`)

CPU affinity pinning and `rdtscp`-based cycle measurements are used to reduce scheduler noise and improve timestamp stability.

This benchmark intentionally focuses on **single-threaded hot-path performance** rather than long-term durability, compaction behavior, or multi-threaded throughput.

---

## Environment

- Architecture : x86_64
- CPU Model : AMD EPYC-Rome Processor (virtualized)
- vCPU : 2 (1 core × 2 threads)
- Hypervisor : Microsoft Hyper-V (full virtualization)
- NUMA : Single node
- Caches:
  - L1d : 32 KiB
  - L1i : 32 KiB
  - L2 : 512 KiB
  - L3 : 16 MiB
Rust : 1.90.1
Build : cargo run --release --features "rdtscp affinity"
CPU Governor : performance
CPU Affinity : Pinned to single logical core
Timestamp Source : rdtscp


---

## Methodology

### Dataset

- Number of operations (N): **1,000,000**
- Value size: **1024 bytes**
- Key distribution:
  - Sequential: monotonically increasing keys
  - Random: uniformly shuffled keys
- redb batch size: **2000**

---

### Measurement

- Aggregation: Median
- Warmup runs: 3
- Measurement runs: 9
- CPU affinity: Enabled (single-core pinning)
- Timestamp source: `rdtscp`
- Reported latency: `elapsed_time / total_accesses`

> All latency values in this report are derived from total elapsed time divided by access count.

---

### Limitations

This benchmark does not measure:

- fsync durability guarantees
- Background compaction overhead
- Multi-threaded contention
- Long-running steady-state behavior

Results should be interpreted as **isolated microbenchmark measurements**.

---

## Results

### Sequential Write
<img width="640" height="480" alt="seq_write" src="https://github.com/user-attachments/assets/5e1e5192-e6bc-416b-a600-88fa3e6334cd" />

| DB     | µs/op |
|--------|--------|
| Kepler | 2.407 |
| sled   | 5.630 |
| redb   | 4.137 |

---

### Random Write
<img width="640" height="480" alt="rand_write" src="https://github.com/user-attachments/assets/b8985364-7e3a-4815-920c-6359c97d685e" />

| DB     | µs/op |
|--------|--------|
| Kepler | 2.690 |
| sled   | 9.476 |
| redb   | 13.107 |

---

### Sequential Read
<img width="640" height="480" alt="seq_read" src="https://github.com/user-attachments/assets/5ef7dbd9-865d-44f8-8916-b7e2b68afd9a" />

| DB     | µs/op |
|--------|--------|
| Kepler | 3.266 |
| sled   | 6.821 |
| redb   | 4.315 |

---

### Random Read
<img width="640" height="480" alt="rand_read" src="https://github.com/user-attachments/assets/3c8c69a8-19f6-4aef-92a8-71da7ffd282e" />

| DB     | µs/op |
|--------|--------|
| Kepler | 9.938 |
| sled   | 7.477 |
| redb   | 4.321 |

---

## Interpretation

- **Write-heavy workloads**
  - Kepler consistently outperforms sled and redb by approximately **1.7×–4.8×**.
  - The performance advantage is most pronounced in random write workloads, suggesting reduced write amplification and cache-friendly internal layout.

- **Sequential read workloads**
  - Kepler maintains a clear advantage over sled and remains competitive with redb.

- **Random read workloads**
  - Kepler is currently slower than redb and sled.
  - This likely reflects differences in index layout, cache locality, bloom filter effectiveness, and page access patterns.
  - Random read performance represents a clear optimization opportunity for future improvements.

Overall, Kepler demonstrates strong write-path efficiency while exposing meaningful optimization targets in read-heavy random access patterns.

---

