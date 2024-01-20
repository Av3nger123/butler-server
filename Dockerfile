FROM golang:1.17

WORKDIR /app

COPY . .

RUN go mod download

RUN go build -o ./target/main ./cmd/main.go

EXPOSE 8080

ENTRYPOINT ["../target/main"]
