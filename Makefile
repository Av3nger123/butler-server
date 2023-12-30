build:
	go build -o butler-server ./cmd/

run: build
	./butler-server

clean:
	rm -f butler-server