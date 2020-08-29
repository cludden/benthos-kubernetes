FROM alpine:edge
RUN apk update && apk upgrade && apk add ca-certificates

RUN addgroup -S benthos && adduser -S benthos -G benthos

COPY --chown=benthos:benthos benthos /benthos

USER benthos
EXPOSE 4195
ENTRYPOINT ["/benthos"]
