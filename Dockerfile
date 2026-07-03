# 多阶段构建，使用 musl 实现静态编译
FROM rust:1.81-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /usr/src/iptv-proxy

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release

COPY src ./src
RUN cargo build --release

# 最终运行镜像
FROM alpine:latest

RUN apk add --no-cache ca-certificates

COPY --from=builder /usr/src/iptv-proxy/target/release/iptv /usr/local/bin/iptv-proxy

EXPOSE 7878
CMD ["iptv-proxy"]
