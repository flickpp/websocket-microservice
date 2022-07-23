FROM rust:latest as BUILDER
WORKDIR /usr/src/websocket-microservice
COPY src src
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
RUN cargo install --path .

FROM debian:bookworm
EXPOSE 8080
EXPOSE 8081
COPY --from=BUILDER /usr/local/cargo/bin/websocket-microservice /usr/local/bin/websocket-microservice
CMD ["websocket-microservice"]