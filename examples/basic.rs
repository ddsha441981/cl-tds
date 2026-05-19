//! Basic CL-TDS usage — count items and query frequencies.

use cl_tds::ClTds;

fn main() {
    // Create a sketch (1 MB fixed memory)
    let sketch = ClTds::new();

    // Simulate a stream: some IPs appear more than others
    let traffic = [
        ("attacker", 10_000u64),
        ("user_a", 500),
        ("user_b", 200),
        ("user_c", 50),
    ];

    // Insert items
    for &(label, count) in &traffic {
        let id = hash(label);
        for _ in 0..count {
            sketch.increment(id);
        }
    }

    // Query estimated frequencies
    println!("╔══════════════════════════════════════╗");
    println!("║  CL-TDS Basic Example                ║");
    println!("╠══════════════════════════════════════╣");
    for &(label, real_count) in &traffic {
        let est = sketch.query(hash(label));
        println!("║  {:<12} real={:<6} est={:<6} {}  ║",
            label, real_count, est,
            if est as u64 == real_count { "✅" } else { "≈" });
    }
    println!("║                                      ║");
    println!("║  Memory: {} bytes (1 MB)        ║", sketch.memory_bytes());
    println!("╚══════════════════════════════════════╝");
}

/// Simple FNV-1a hash for demo purposes.
fn hash(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}
