FROM golang:1.24-alpine AS build

WORKDIR /app

COPY go.* ./
RUN go mod download

COPY . ./

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o lfsproxy ./cmd/server.go

FROM alpine:3.21

COPY --from=build /app/lfsproxy /lfsproxy
USER nobody:nobody
ENTRYPOINT ["/lfsproxy"]
