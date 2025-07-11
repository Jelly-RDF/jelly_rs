include!(concat!(
    env!("OUT_DIR"),
    "/eu.ostrzyciel.jelly.core.proto.v1.rs"
));

pub mod patch {
    include!(concat!(
        env!("OUT_DIR"),
        "/eu.ostrzyciel.jelly.core.proto.v1.patch.rs"
    ));
}
