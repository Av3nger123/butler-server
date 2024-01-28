package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

// LoadEnv loads environment variables from the .env file
func LoadEnv() {
	if err := godotenv.Load(".env.local"); err != nil {
		log.Println("Error loading .env file")
	}
}

// GetString returns the value of an environment variable as a string
func GetString(key string) string {
	return os.Getenv(key)
}
