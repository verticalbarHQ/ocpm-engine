FROM rust:1.85.1-bookworm AS builder

ARG TARGETARCH
ARG DUCKDB_VERSION=1.5.5
ARG DUCKDB_AMD64_SHA256=1fb8ce388157d84a25abe685a8a2520bf00c00321821968e4bb398fd766e7abb
ARG DUCKDB_ARM64_SHA256=abe4f6f005ee0b448a058322f4263584b4bd1b6faf7ab4637b79eeaf978f8e9c

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       binutils \
       ca-certificates \
       curl \
       python3 \
       python3-venv \
       unzip \
    && rm -rf /var/lib/apt/lists/*

RUN case "$TARGETARCH" in \
      amd64) asset=libduckdb-linux-amd64.zip; expected="$DUCKDB_AMD64_SHA256" ;; \
      arm64) asset=libduckdb-linux-arm64.zip; expected="$DUCKDB_ARM64_SHA256" ;; \
      *) echo "unsupported wheel architecture: $TARGETARCH" >&2; exit 2 ;; \
    esac \
    && curl -fsSL -o /tmp/libduckdb.zip \
       "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/${asset}" \
    && echo "$expected  /tmp/libduckdb.zip" | sha256sum --check --strict \
    && unzip -q /tmp/libduckdb.zip libduckdb.so duckdb.h -d /tmp/libduckdb \
    && install -m 0644 /tmp/libduckdb/libduckdb.so /usr/local/lib/libduckdb.so \
    && install -m 0644 /tmp/libduckdb/duckdb.h /usr/local/include/duckdb.h \
    && ldconfig

RUN python3 -m venv /opt/venv \
    && /opt/venv/bin/python -m pip install --no-cache-dir 'maturin==1.14.1'

ENV PATH=/opt/venv/bin:$PATH \
    DUCKDB_INCLUDE_DIR=/usr/local/include \
    DUCKDB_LIB_DIR=/usr/local/lib \
    LD_LIBRARY_PATH=/usr/local/lib \
    RUSTUP_TOOLCHAIN=1.85.1

WORKDIR /src/ocpm-engine
COPY . ./
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/ocpm-engine/target \
    PYTHON=python DIST_DIR=/out ./scripts/build-private-wheel.sh

FROM scratch AS artifact
COPY --from=builder /out/ /
