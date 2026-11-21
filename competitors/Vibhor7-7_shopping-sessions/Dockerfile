# Shopping Sessions: a single static binary, built from source.
# No FROM line pins a platform: the scorer builds this natively on linux/amd64,
# and `make build PLATFORM=linux/arm64` builds it natively on an Apple-silicon Mac.

FROM rust:1.100-alpine3.22 AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Not built into the image; the manifest names it, so Cargo wants to see it.
COPY ref ./ref
# The crate has no dependencies, so the build needs nothing from the network.
# The scorers are Cascade Lake or newer: x86-64-v3 (AVX2, BMI2) is always there.
RUN set -eu; \
    case "$(uname -m)" in x86_64) export RUSTFLAGS="-C target-cpu=x86-64-v3" ;; esac; \
    cargo build --release --locked --offline --bin pipeline; \
    mkdir -p /rootfs/app /rootfs/data /rootfs/output /rootfs/scratch; \
    cp target/release/pipeline /rootfs/app/pipeline

# At run time the root filesystem is read-only, the user is 1000:1000 and there
# is no network. The binary is static and writes only to $OUTPUT_DIR.
#
# The runtime image is as small as an image with a shell can be (2 MB): if a
# sandbox is unpacked from the image for every run, its size is paid on every
# run. BusyBox rather than scratch so that a harness that runs `sh` or `chmod`
# inside the image still can; the pipeline itself needs neither, and creates
# its output readable, writable and removable by anyone.
FROM busybox:1.37-musl
# One layer: the binary, and the mount points should the sandbox not create them.
COPY --from=build /rootfs/ /
WORKDIR /app
CMD ["/app/pipeline"]
