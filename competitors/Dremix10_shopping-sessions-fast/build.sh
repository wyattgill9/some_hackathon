#!/bin/sh
# Native build for local iteration: ./build.sh [extra flags]
set -e
cd "$(dirname "$0")"
mkdir -p build
c++ -O3 -mcpu=native -std=c++20 -pthread -Wall -Wextra -Wno-unused-parameter -Wno-missing-field-initializers "$@" -o build/sessions src/main.cpp src/fallback.cpp
