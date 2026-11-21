# Shopping Sessions pipeline: one static Rust binary in an empty image.
# Builder: the same Rust release the code was developed and tested with.
FROM rust:1.97-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
# Scorers are Intel Xeon Cascade Lake or newer: AVX-512BW/VL and BMI2 are available.
# A static binary needs nothing from the runtime image.
RUN RUSTFLAGS="-C target-cpu=cascadelake -C target-feature=+crt-static" \
    cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/sessions /sessions

FROM scratch
COPY --from=build /sessions /sessions
# The scorer runs as uid 1000 with a read-only root; the binary writes only to $OUTPUT_DIR.
USER 1000:1000
ENV DATA_DIR=/data OUTPUT_DIR=/output
CMD ["/sessions"]
