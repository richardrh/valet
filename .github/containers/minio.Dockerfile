FROM scratch
LABEL org.opencontainers.image.source="https://github.com/minio/minio" \
      org.opencontainers.image.revision="07c3a429bfed" \
      org.opencontainers.image.licenses="AGPL-3.0-only"
COPY minio /minio
COPY LICENSE /LICENSE
ENV HOME=/tmp MINIO_BROWSER=off MINIO_UPDATE=off
USER 65532:65532
ENTRYPOINT ["/minio"]
CMD ["server", "/data", "--address", ":9000"]
