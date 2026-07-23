# Transaction Manager — server image
#
# Multi-stage build: the first stage compiles a static binary from a Go
# toolchain; the second stage copies only the binary into a distroless
# (non-root) runtime image. The resulting image has no shell, no package
# manager, and a tiny attack surface.
#
# Build:
#   docker build -t txn-manager:latest .
# Run:
#   docker run --rm -p 8080:8080 \
#     -e LISTEN_ADDR=:8080 \
#     -e ADMIN_TOKEN=... \
#     -e CORS_ALLOW_ORIGINS=https://app.example.com \
#     txn-manager:latest

# ─── Build stage ─────────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS build
WORKDIR /src

# Cache module downloads separately from the source.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Static build: CGO_ENABLED=0 strips libc dependency so the runtime
# image can be distroless. -buildvcs=false avoids VCS-stamp failures on
# shallow CI checkouts (L-INF-21).
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -buildvcs=false -ldflags="-s -w" \
    -o /out/server ./cmd/server

# ─── Runtime stage ───────────────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /out/server /server

# Non-root (uid 65532) per distroless:nonroot; expose the default listen
# port and set a sane default.
EXPOSE 8080
ENV LISTEN_ADDR=:8080

# distroless has no shell, so the entrypoint is the binary directly.
ENTRYPOINT ["/server"]
