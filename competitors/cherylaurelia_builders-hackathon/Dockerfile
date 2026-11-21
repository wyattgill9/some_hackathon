# Shopping Sessions: native Rust pipeline. Scorers are linux/amd64 Intel Xeon (Cascade Lake or newer).
FROM rust:1.91.1-slim-bookworm AS build
ARG TARGET_CPU=x86-64-v3
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN RUSTFLAGS="-C target-cpu=${TARGET_CPU}" cargo build --release --locked \
 && cp target/release/sessions /sessions

FROM debian:bookworm-slim
COPY --from=build /sessions /usr/local/bin/sessions
USER 1000:1000
CMD ["/usr/local/bin/sessions"]
