IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
# The scorer's platform. PLATFORM=linux/arm64 builds and runs natively on an Apple-silicon Mac.
PLATFORM ?= linux/amd64
DEV   ?= dev
ANSWER ?= dev-answer
# cargo from the PATH, or where rustup puts it when the PATH was left alone.
CARGO ?= $(shell command -v cargo 2>/dev/null || echo $(HOME)/.cargo/bin/cargo)
# The test scripts need Python 3.10+ with polars; uv supplies both without touching the system Python.
PY    ?= uv run --quiet --python 3.12 --with polars python
export IMAGE CPUS MEM PLATFORM

.PHONY: build fixtures dev run-dev check-dev check-out native test native-fixtures reference diff regress

## Build the image for the scorer's platform, linux/amd64 (emulated on Apple silicon)
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Run the image on all 36 edge-case fixtures and diff against the expected tables
fixtures: build
	$(PY) scripts/run_fixtures.py fixtures -- scripts/docker_run.sh

## Make the 1M-event dev set with the published generator (needs `generate` on PATH or in .tools/)
dev:
	PATH="$(CURDIR)/.tools:$$PATH" generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

## Run on the dev set with scorer flags and time it
run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

## The dev set through the native build, compared with the published dev answer (dev-answer.zip, unzipped here).
## After `make run-dev`, `make check-out` compares what the image wrote instead.
check-dev: native
	rm -rf out && target/release/pipeline $(DEV) out 2>/dev/null && $(PY) scripts/compare.py out $(ANSWER)

check-out:
	$(PY) scripts/compare.py out $(ANSWER)

## The same checks without Docker, on a native build: the fast loop
native:
	$(CARGO) build --release

test:
	$(CARGO) test --release

native-fixtures: native
	$(PY) scripts/run_fixtures.py fixtures -- target/release/pipeline

## The slow oracle against the fixtures and the dev answer
reference: native
	scripts/check_reference.sh

## Pipeline against the reference (ref/) on seeds with no published answer: make diff SEEDS="2 3 4" SIZE=1M
SEEDS ?= 2 3 4 5
SIZE ?= 200000
diff: native
	scripts/diff_vs_ref.sh "$(SEEDS)" $(SIZE)

## Everything that must hold before a commit is handed to the scorer
regress:
	scripts/regress.sh
