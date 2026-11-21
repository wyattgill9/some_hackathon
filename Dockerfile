# Shopping Sessions: one static Rust binary in an empty image (see src/main.rs). Build time is not scored.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
COPY generator ./generator
# Scorers are Intel Xeon Cascade Lake: tune for it, keep jumps off 32-byte boundaries (the JCC erratum
# mitigation empties the uop cache otherwise), and link statically so nothing is loaded at run time.
# No AVX-512: a single 512-bit instruction drops the core's clock for a while, and the hot loops are AVX2.
ENV BASE="-C target-cpu=cascadelake -C target-feature=+crt-static,-avx512f -C llvm-args=-x86-branches-within-32B-boundaries"
ENV TARGET="x86_64-unknown-linux-gnu"
# Profile-guided build: an instrumented binary runs on a 2M-event set from the track's own generator, and
# the final binary is compiled with that profile. Any failure along the way falls back to a plain build.
RUN (rustup component add llvm-tools \
     && RUSTFLAGS="$BASE -Cprofile-generate=/tmp/pgo" cargo build --release --locked --target $TARGET --target-dir /tmp/instrumented \
     && rustc --edition 2024 -O -C panic=abort generator/src/main.rs -o /tmp/generate \
     && /tmp/generate --seed 11 --events 2000000 --clicks 500000 --output /tmp/train >/dev/null \
     && mkdir -p /tmp/out \
     && DATA_DIR=/tmp/train OUTPUT_DIR=/tmp/out NCPU=8 /tmp/instrumented/$TARGET/release/pipeline \
     && "$(rustc --print sysroot)/lib/rustlib/$TARGET/bin/llvm-profdata" merge -o /tmp/pgo.profdata /tmp/pgo/*.profraw \
     && RUSTFLAGS="$BASE -Cprofile-use=/tmp/pgo.profdata" cargo build --release --locked --target $TARGET \
     && echo "PGO build") \
 || (echo "PGO failed: plain build" && RUSTFLAGS="$BASE" cargo build --release --locked --target $TARGET)
RUN cp target/$TARGET/release/pipeline /pipeline

FROM scratch
COPY --from=build /pipeline /pipeline
# The scorer runs as uid 1000 with a read-only root FS; the binary reads $DATA_DIR and writes only $OUTPUT_DIR.
USER 1000:1000
CMD ["/pipeline"]
