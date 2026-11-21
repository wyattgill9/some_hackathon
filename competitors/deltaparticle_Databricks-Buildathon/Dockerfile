# Shopping Sessions: one statically linked C++ binary in an empty image.
# The pipeline in cpp/ is adapted from github.com/Dremix10/shopping-sessions-fast
# (see README). The scorer builds this natively for linux/amd64 and runs CMD as
# uid 1000 with a read-only root, no network, /data read-only, /output writable.
FROM gcc:14-bookworm AS build
WORKDIR /src
COPY cpp/ /src/cpp/
# Scorers are Intel Xeon, Cascade Lake or newer: AVX2/BMI2 always, AVX-512 checked at run time.
RUN g++ -O3 -march=x86-64-v3 -mtune=skylake-avx512 -std=c++20 -pthread -static -fno-plt \
      -o /sessions cpp/main.cpp cpp/fallback.cpp

FROM scratch
COPY --from=build /sessions /sessions
ENV DATA_DIR=/data OUTPUT_DIR=/output NCPU=8
USER 1000:1000
CMD ["/sessions"]
