# Shopping Sessions: one static-ish Rust binary (see src/main.rs). Build time is not scored; only the run is.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# x86-64-v3 (AVX2) runs on the Cascade Lake scorers and on any recent dev box.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked && cp target/release/pipeline /pipeline

FROM debian:bookworm-slim
COPY --from=build /pipeline /pipeline
# Runs as uid 1000 with a read-only root FS; it only reads $DATA_DIR and writes $OUTPUT_DIR.
CMD ["/pipeline"]
