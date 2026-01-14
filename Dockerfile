FROM golang:1.24-alpine AS builder

ENV GOTOOLCHAIN=auto
WORKDIR /src

# Copy go-gfs dependency (provided by build context)
COPY go-gfs /go-gfs

# Copy cluster-monitor source
COPY cluster-monitor/go.mod cluster-monitor/go.sum /src/
COPY cluster-monitor /src/

# Update replace directive to match Docker build context paths
RUN sed -i 's|replace eddisonso.com/go-gfs => ../go-gfs|replace eddisonso.com/go-gfs => /go-gfs|' go.mod

RUN CGO_ENABLED=0 go build -o /out/cluster-monitor .

FROM alpine:3.20
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /out/cluster-monitor .
ENTRYPOINT ["./cluster-monitor"]
