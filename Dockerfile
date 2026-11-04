FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/webhookd ./cmd/webhookd \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/webhook-receiver ./cmd/webhook-receiver

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/webhookd /out/webhook-receiver /
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/webhookd"]
