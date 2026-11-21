# Static C++ binary, shipped alone. Scorers are Intel Xeon, Cascade Lake or newer.
FROM gcc:14-bookworm@sha256:5e927c284bf55a7dc796262e311a0703344f62f41f5621eb56843111b1d37e15 AS build
ARG TARGETARCH
WORKDIR /src
COPY src/ /src/src/
# -mbranches-within-32B-boundaries keeps jumps off 32-byte boundaries, where
# Skylake-family cores (Cascade Lake included) fall out of the decoded-uop cache.
RUN if [ "$TARGETARCH" = "arm64" ]; then ARCH="-mcpu=native"; else ARCH="-march=x86-64-v3 -mtune=skylake-avx512 -Wa,-mbranches-within-32B-boundaries"; fi; \
    g++ -O3 -flto $ARCH -std=c++20 -pthread -static -fno-plt -o /sessions src/main.cpp src/fallback.cpp

FROM scratch
COPY --from=build /sessions /sessions
CMD ["/sessions"]
