use std::fs::File;

use jelly::{
    FrameReader,
    deserialize::Deserializer,
    to_rdf::{StringRdf, ToRdf},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init(); // Initialize logger, reads RUST_LOG env var
    let args: Vec<_> = std::env::args().into_iter().collect::<Vec<_>>();
    let file_name = args.get(1).expect("A file argument");
    let file = File::open(file_name)?;
    let generator = FrameReader::new(file);

    let mut des = Deserializer::<StringRdf>::new(());

    let mut h = (
        |(s, p, o): <StringRdf as ToRdf>::Triple| println!("Triple {} {} {} .", s, p, o),
        |(s, p, o, g): <StringRdf as ToRdf>::Quad| {
            if let Some(g) = g {
                println!("Quad {} {} {} {} .", s, p, o, g);
            } else {
                println!("Quad {} {} {} .", s, p, o);
            }
        },
    );
    for frame in generator {
        des.handle_frame(frame, &mut h)?;
    }

    Ok(())
}
