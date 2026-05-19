<div align="center">

# CL-TDS

**Cache-Locked Temporal Decay Sketch**

*Find what's happening most in a data stream — using only 1 MB of memory.*

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![CI](https://github.com/ddsha441981/cl-tds/actions/workflows/ci.yml/badge.svg)](https://github.com/ddsha441981/cl-tds/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/cl-tds.svg)](https://crates.io/crates/cl-tds)
[![docs.rs](https://docs.rs/cl-tds/badge.svg)](https://docs.rs/cl-tds)
[![Rust](https://img.shields.io/badge/Rust-1.56+-orange.svg)](https://www.rust-lang.org)
[![Dependencies](https://img.shields.io/badge/Dependencies-0-brightgreen.svg)](#)
[![Memory](https://img.shields.io/badge/Memory-1_MB_fixed-purple.svg)](#)

[Documentation](https://docs.rs/cl-tds) · [Getting Started](#getting-started) · [How It Works](#how-it-works) · [Benchmarks](#benchmarks) · [API Reference](#api-reference)

</div>

---

CL-TDS is a Rust library for real-time heavy-hitter detection in data streams. It tracks which items appear most frequently — right now, not yesterday — with old data fading automatically. Memory never grows beyond 1 MB. No external dependencies. Thread-safe without locks.

```rust
use cl_tds::ClTds;

let sketch = ClTds::with_epoch_interval(1000); // auto-decay every second

sketch.increment(to_id("attacker_ip"));
sketch.increment(to_id("attacker_ip"));
sketch.increment(to_id("normal_user"));

if sketch.query(to_id("attacker_ip")) > 10_000 {
    // this IP is flooding — block it
}
```

---

## ⚡ Why CL-TDS?

<table>
<tr>
<td width="50%">

### 🧠 1 MB Forever
HashMap grows with your data. CL-TDS doesn't.

| Unique Items | HashMap | CL-TDS |
|---|---|---|
| 100K | 1.9 MB | **1 MB** |
| 1M | 29.8 MB | **1 MB** |
| 5M | 119 MB | **1 MB** |
| 10M | 238 MB | **1 MB** |

At 10M items, CL-TDS uses **238x less memory**.

</td>
<td width="50%">

### 🚀 Faster Than HashMap
Fits in L3 cache → every access is a cache hit.

| Operation | CL-TDS | HashMap |
|---|---|---|
| Insert (1M) | **26.1M/s** | 14.9M/s |
| Query (1M) | **49.4M/s** | 10.4M/s |
| 100M inserts | **33.0M/s** | OOM risk |
| 4-thread 40M | **43.4M/s** | N/A |

**1.8x** faster insert, **4.8x** faster query.

</td>
</tr>
</table>

### 🕐 Temporal Decay — Old Data Fades Automatically

HashMap remembers everything forever. If an attacker was active yesterday but stopped today, HashMap still shows a million hits — a **stale false alert**.

CL-TDS forgets. Every epoch, counts halve. After 24 epochs, old data reaches zero.

```
After 24 epochs of silence:
  HashMap:  1,000,000  ← stale data, triggers false alert
  CL-TDS:  0          ← correctly forgotten, no false alert
```

### 🔒 Thread-Safe Without Locks

All operations use atomic compare-and-swap. No mutexes, no `RwLock`, no contention. Five sketches (5 MB) run on 5 threads simultaneously — all fitting in L3 cache with zero speed loss.

### 📦 Zero Dependencies

Built entirely on `std::sync::atomic`. No crates, no C bindings, no build complications.

---

## Getting Started

Add to your `Cargo.toml`:

```toml
[dependencies]
cl-tds = { git = "https://github.com/ddsha441981/cl-tds" }
```

### Manual Decay

You control when data ages:

```rust
use cl_tds::ClTds;

let sketch = ClTds::new();

sketch.increment(42);
sketch.increment(42);
assert!(sketch.query(42) >= 2);

sketch.tick_epoch(); // all counts halve on next touch
```

### Auto Decay

Data ages based on real time — no manual ticking:

```rust
use cl_tds::ClTds;

let sketch = ClTds::with_epoch_interval(1000); // 1 epoch = 1 second
sketch.increment(42);
// after 1 second, count halves automatically
// after 24 seconds, count reaches 0
```

### Hashing Your Data

CL-TDS works with `u64` identifiers. Hash any type:

```rust
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

fn to_id<T: Hash>(item: &T) -> u64 {
    let mut h = DefaultHasher::new();
    item.hash(&mut h);
    h.finish()
}

sketch.increment(to_id(&"192.168.1.1"));    // IP address
sketch.increment(to_id(&"POST /api/pay"));  // API endpoint
sketch.increment(to_id(&"#trending"));      // hashtag
sketch.increment(to_id(&12345u64));         // user ID
```

### Persistence

Save to disk and restore after restart:

```rust
// Save
let bytes = sketch.to_bytes();
std::fs::write("snapshot.bin", &bytes).unwrap();

// Restore
let data = std::fs::read("snapshot.bin").unwrap();
let restored = ClTds::from_bytes(&data).unwrap();
```

---

## How It Works

CL-TDS is a Count-Min Sketch with two key innovations:

**1. Cache-Locked Matrix** — The data structure is exactly 1 MB (4 rows × 65536 columns × 4 bytes), designed to permanently reside in L3 cache. Every memory access is a cache hit.

**2. Lazy Temporal Decay** — Each cell packs an 8-bit timestamp and 24-bit counter into one `u32`. When touched, stale cells are decayed by right-shifting the counter. No background thread, no global sweep — O(1) per operation.

```
┌─────────────────────────────────────────────────┐
│  4 Rows × 65536 Columns × 4 bytes = 1 MB       │
│                                                 │
│  Row 0: [ts|count] [ts|count] ... (65536 cells) │
│  Row 1: [ts|count] [ts|count] ... (65536 cells) │
│  Row 2: [ts|count] [ts|count] ... (65536 cells) │
│  Row 3: [ts|count] [ts|count] ... (65536 cells) │
│                                                 │
│  Each cell: [8-bit timestamp | 24-bit counter]  │
│  Hash:  4 pairwise-independent (random seeds)   │
│  CAS:   lock-free atomic compare-and-swap       │
│  Decay: lazy right-shift on touch               │
└─────────────────────────────────────────────────┘
```

### Mathematical Guarantees

These are proven bounds, not estimates:

| Theorem | Formula | Bound |
|---|---|---|
| Error Bound | `E[query(x)] ≤ f(x) + ε · N_effective` | ε = e/65536 ≈ 0.0000414 |
| False Positive | `P[false positive] ≤ δ` | δ = e⁻⁴ ≈ 1.8% |
| Decay Equivalence | `V_lazy(C) = V_full(C)` | Exact equality |

> **In practice:** With 10M background flows, all 5 hidden DDoS attackers were detected with **0 false positives**.

---

## Benchmarks

### CL-TDS vs HashMap — Extreme Scale

| Test | Scale | Result |
|---|---|---|
| Zipf distribution | 50M inserts | Both handle it. CL-TDS: 1 MB. HashMap: grows. |
| Pure throughput | 100M inserts | CL-TDS: 33M ops/sec, 1 MB. HashMap: would need 1.2 GB. |
| Memory at scale | 10M unique items | HashMap: 238 MB. CL-TDS: **1 MB (238x less)**. |
| DDoS detection | 10.5M packets | 5/5 attackers found, 0 false positives, both systems. |
| Temporal decay | 1M stale items | HashMap: stale alert. CL-TDS: correctly forgotten. |
| Multi-threaded | 4 threads × 10M | 43.4M aggregate ops/sec, lock-free, 1 MB fixed. |

### Memory Growth Curve

```
    HashMap Memory          CL-TDS Memory
    ──────────────          ─────────────
    10K  →    238 KB        10K  →  1 MB
    100K →    1.9 MB        100K →  1 MB
    1M   →   29.8 MB        1M   →  1 MB
    5M   →    119 MB        5M   →  1 MB
    10M  →    238 MB        10M  →  1 MB ← never changes
```

---

## Where It Works

CL-TDS fits any problem shaped like: *"What's appearing most in this stream, right now, with bounded memory?"*

### 17 Domains — Benchmarked and Proven

| Domain | Use Case | Scale Tested |
|---|---|---|
| 🛡️ Network Security | DDoS attacker IP detection | 27.7M flows (9.2 GB) |
| 💹 Financial Trading | Most traded price levels (HFT) | 4.7M trades (Binance) |
| 🛒 Web Analytics | Trending products/brands | 1.6K reviews (Amazon) |
| 🎮 Gaming | Anti-cheat (speed hacks, aimbot) | 5 cheaters in 10K players |
| 🌐 IP Flood Detection | Port scanning patterns | 10 attackers × 200K packets |
| 🔍 DNS Monitoring | Malware C2 domain detection | 3 malware + 5K legit domains |
| ⚙️ API Rate Limiting | Abusive user/key detection | 3 abusers + 20K users |
| 🖱️ Click Fraud | Ad-tech bot detection | 3 bots + 50K real users |
| 📡 CDN Caching | Hot URL identification | 3 viral + 100K pages |
| 📞 Telecom | SIM-box fraud detection | 3 fraudsters + 50K callers |
| 🌡️ IoT Sensors | Faulty sensor alerting | 3 faulty + 10K sensors |
| 📋 Log Analysis | Top failing endpoints | 3 errors + 30K logs |
| 📧 Spam Detection | Bulk sender identification | 3 spammers + 100K senders |
| 📱 Social Trending | Viral hashtag detection | 4 viral + 200K hashtags |
| 🔎 Search Tracking | Trending query detection | 3 surging + 500K queries |
| ⚡ Smart Grid | Overloaded transformer alerts | 3 faults + 20K events |
| 💳 Fintech Fraud | Suspicious card/UPI patterns | 3 fraud + 100K transactions |

### When NOT to Use CL-TDS

| Scenario | Why Not | Use Instead |
|---|---|---|
| Exact counting (ledgers, voting) | CL-TDS is approximate | HashMap, database |
| Listing all unique items | Tracks frequencies only | HyperLogLog |
| Item deletion (GDPR) | Hash collisions prevent removal | Counting Bloom filter |
| Small datasets (< 100K) | Overkill | HashMap |
| Relationship detection (X → Y) | Single-item counting only | Graph databases |
| Sequence/pattern matching | No concept of order | Regex, FSM |

---

## Testing

```bash
cargo test   # 40 unit tests + 14 doc tests — all passing
```

Run the bundled examples:

```bash
cargo run --example basic         # insert, query, basics
cargo run --example decay         # temporal decay in action
cargo run --example multithread   # thread-safety demo
cargo run --example persistence   # save/restore to disk
```

---

## API Reference

| Method | Description |
|---|---|
| `ClTds::new()` | Manual mode — you call `tick_epoch()` to advance decay |
| `ClTds::with_epoch_interval(ms)` | Auto mode — decay ticks based on wall clock |
| `ClTds::new_deterministic()` | Fixed hash seeds for reproducible tests |
| `sketch.increment(id)` | Record one occurrence of `id` in the stream |
| `sketch.query(id)` | Get estimated frequency (minimum across 4 rows) |
| `sketch.tick_epoch()` | Advance decay clock by one step (manual mode) |
| `sketch.epoch()` | Current epoch number |
| `sketch.to_bytes()` | Serialize full state (~1 MB) for crash recovery |
| `ClTds::from_bytes(data)` | Restore from saved bytes |
| `sketch.memory_bytes()` | Always returns 1,048,576 |
| `ClTds::algorithm_parameters()` | Returns (ε, δ, width, depth) |
| `ClTds::error_bound(n)` | Worst-case overcount for stream of n items |

---

## Author

<table>
<tr>
<td>

**Deendayal Kumawat**

[![Email](https://img.shields.io/badge/Email-deendayal__kumawat%40outlook.com-blue?logo=microsoft-outlook)](mailto:deendayal_kumawat@outlook.com)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Deendayal_Kumawat-blue?logo=linkedin)](https://www.linkedin.com/in/deendayal-kumawat/)
[![GitHub](https://img.shields.io/badge/GitHub-ddsha441981-black?logo=github)](https://github.com/ddsha441981)

</td>
</tr>
</table>

---

## License

```
Copyright 2026 Deendayal Kumawat

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
```

See [LICENSE](LICENSE) for the full text.
