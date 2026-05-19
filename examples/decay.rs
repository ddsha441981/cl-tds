//! Temporal decay example — old data fades automatically.

use cl_tds::ClTds;
use std::thread;
use std::time::Duration;

fn main() {
    // Auto-decay: 1 epoch = 500ms → data halves every 500ms
    let sketch = ClTds::with_epoch_interval(500);

    // Insert a burst of traffic
    let id = 0xDEAD_BEEF;
    for _ in 0..1000 {
        sketch.increment(id);
    }

    println!("╔══════════════════════════════════════╗");
    println!("║  CL-TDS Temporal Decay Demo          ║");
    println!("╠══════════════════════════════════════╣");
    println!("║  Initial count: {:<20}  ║", sketch.query(id));

    // Watch it decay over time
    for i in 1..=8 {
        thread::sleep(Duration::from_millis(500));
        let count = sketch.query(id);
        println!("║  After {}s:  count = {:<16}  ║", i as f64 * 0.5, count);
        if count == 0 {
            println!("║  → Fully decayed!                    ║");
            break;
        }
    }
    println!("╚══════════════════════════════════════╝");
}
