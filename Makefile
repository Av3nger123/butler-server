build:
	go build -o ./target/butler-server ./cmd/

run: build
	./target/butler-server

clean:
	rm -f butler-server