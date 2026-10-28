FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG BUILDTIME=unknown

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -tags netgo -trimpath \
    -ldflags "-s -w -X main.Version=${VERSION} -X main.Buildtime=${BUILDTIME}" \
    -o /out/hyper-proxy ./cmd/hyper-proxy

FROM alpine

RUN apk add --no-cache ca-certificates tzdata

COPY --from=build /out/hyper-proxy /hyper-proxy

EXPOSE 8092

ENTRYPOINT ["/hyper-proxy"]
