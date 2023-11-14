package service

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
)

const nonceSize = 12

func Decrypt(data string, key []byte) (string, error) {
	cipherText, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return "", err
	}

	// Extract nonce, encrypted payload, and tag
	nonce := cipherText[:12]
	encryptedPayload := cipherText[12 : len(cipherText)-16]
	tag := cipherText[len(cipherText)-16:]

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	// Create a GCM cipher
	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	// Decrypt the payload
	decryptedPayload, err := aesGCM.Open(nil, nonce, encryptedPayload, tag)
	if err != nil {
		return "", err
	}

	return string(decryptedPayload), nil
}
