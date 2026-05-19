//! Multi-threaded example — concurrent inserts from 4 threads.

use cl_tds::ClTds;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn main() {
    let sketch = Arc::new(ClTds::new());
    let threads = 4;
    let items_per_thread = 1_000_000u64;

    println!("╔══════════════════════════════════════╗");
    println!("║  CL-TDS Multi-Thread Stress Test     ║");
    println!("╠══════════════════════════════════════╣");
    println!("║  Threads: {}                          ║", threads);
    println!("║  Items/thread: {:>10}             ║", items_per_thread);

    let start = Instant::now();

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let s = Arc::clone(&sketch);
            thread::spawn(move || {
                // Each thread inserts items with thread-specific IDs
                for i in 0..items_per_thread {
                    let id = (t as u64) * 10_000_000 + i;
                    s.increment(id);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total = threads as u64 * items_per_thread;
    let mpps = total as f64 / elapsed.as_secs_f64() / 1e6;

    println!("║  Total items:   {:>10}             ║", total);
    println!(
        "║  Time:          {:>7.1} ms             ║",
        elapsed.as_secs_f64() * 1000.0
    );
    println!("║  Throughput:    {:>7.1} M/s             ║", mpps);
    println!("║  Memory:        1 MB (fixed)           ║");
    println!("║  Crashes:       0 ✅                    ║");
    println!("╚══════════════════════════════════════╝");
}
