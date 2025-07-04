FROM rust:1.79.0-bookworm as builder
WORKDIR /usr/src
RUN cargo new --bin ddp-router
COPY Cargo.toml Cargo.lock /usr/src/ddp-router/
WORKDIR /usr/src/ddp-router
RUN cargo build --release
COPY src /usr/src/ddp-router/src
RUN touch /usr/src/ddp-router/src/main.rs
RUN cargo build --release

FROM debian:bookworm
RUN apt-get update && \
    apt-get install --yes openssl ca-certificates && \
    update-ca-certificates
COPY --from=builder /usr/src/ddp-router/target/release/ddp-router /ddp-router
CMD ["/ddp-router"]
