# syntax=docker/dockerfile:1

FROM golang:1.24-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
	go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/refleks-worker ./cmd/worker

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=builder /out/refleks-worker /app/refleks-worker

USER nonroot:nonroot
ENTRYPOINT ["/app/refleks-worker"]
