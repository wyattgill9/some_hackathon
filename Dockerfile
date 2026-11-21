# Shopping Sessions: one static Rust binary in an empty image (see src/main.rs). Build time is not scored.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
# Scorers are Intel Xeon Cascade Lake or newer: x86-64-v3 (AVX2, BMI2) is always there. Static: no loader at run time.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/pipeline /pipeline

FROM scratch
COPY --from=build /pipeline /pipeline
# The scorer runs as uid 1000 with a read-only root FS; the binary reads $DATA_DIR and writes only $OUTPUT_DIR.
USER 1000:1000
CMD ["/pipeline"]
