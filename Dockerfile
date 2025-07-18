# Etapa 1: Build
FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY main.go ./
RUN go build -o app main.go

# Etapa 2: Imagem final leve
FROM alpine:3.20

WORKDIR /app

# Copia o binário
COPY --from=builder /app/app .

# Copia arquivos auxiliares para a pasta de trabalho da imagem final
COPY camercatil.crt ./
COPY .env ./

# Instala dependências para SFTP e certificados
RUN apk add --no-cache ca-certificates openssh-client

# Variável de ambiente padrão do caminho do certificado
ENV KAFKA_CA_CERT_PATH="/app/camercatil.crt"

ENTRYPOINT ["./app"]
