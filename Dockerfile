FROM golang:1.26.10-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=${VERSION}" -o /chalert ./cmd/chalert

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /chalert /chalert
USER nonroot:nonroot
ENTRYPOINT ["/chalert"]