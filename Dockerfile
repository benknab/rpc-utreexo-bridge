FROM rust:1.85.0-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ make pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ARG CARGO_BUILD_JOBS=4
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /app/data && chown 101:101 /app/data

ARG BRIDGE_REVISION=unknown
LABEL org.opencontainers.image.revision=$BRIDGE_REVISION

WORKDIR /app

COPY --from=build /app/target/release/bridge /app/bridge

EXPOSE 3000 8333

VOLUME /app/data

ENV DATA_DIR=/app/data
ENV API_HOST=0.0.0.0:3000

USER 101:101

ENTRYPOINT ["/app/bridge"]
