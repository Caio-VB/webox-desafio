# db/Dockerfile
FROM postgres:16-alpine

# Copia o init.sql que está na mesma pasta do Dockerfile
COPY init.sql /docker-entrypoint-initdb.d/01-webox-schema.sql