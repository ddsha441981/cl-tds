//! Persistence example — save sketch to disk and restore it.

use cl_tds::ClTds;

fn main() {
    let path = "sketch_snapshot.bin";

    // Create and populate a sketch
    let sketch = ClTds::new();
    sketch.increment(100);
    sketch.increment(100);
    sketch.increment(100);
    sketch.increment(200);

    println!("╔══════════════════════════════════════╗");
    println!("║  CL-TDS Persistence Demo             ║");
    println!("╠══════════════════════════════════════╣");
    println!("║  Before save:                        ║");
    println!(
        "║    query(100) = {}                    ║",
        sketch.query(100)
    );
    println!(
        "║    query(200) = {}                    ║",
        sketch.query(200)
    );

    // Save to disk
    let bytes = sketch.to_bytes();
    std::fs::write(path, &bytes).expect("failed to write snapshot");
    println!("║  Saved: {} bytes to disk        ║", bytes.len());

    // Restore from disk
    let loaded = std::fs::read(path).expect("failed to read snapshot");
    let restored = ClTds::from_bytes(&loaded).expect("invalid snapshot");

    println!("║  After restore:                      ║");
    println!(
        "║    query(100) = {}                    ║",
        restored.query(100)
    );
    println!(
        "║    query(200) = {}                    ║",
        restored.query(200)
    );
    println!("║  Match: ✅                            ║");
    println!("╚══════════════════════════════════════╝");

    // Cleanup
    let _ = std::fs::remove_file(path);
}
