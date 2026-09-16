use satel_integra::state::TroubleType;

fn main() {
    println!("{{");
    let mut first = true;
    for desc in TroubleType::catalog() {
        if desc.has_state {
            if !first { println!(","); }
            print!("  \"{}\": \"{}\"", desc.key, desc.label_en);
            first = false;
        }
        if desc.has_memory {
            if !first { println!(","); }
            print!("  \"{}_memory\": \"{} (Memory)\"", desc.key, desc.label_en);
            first = false;
        }
    }
    println!("\n}}");
}
