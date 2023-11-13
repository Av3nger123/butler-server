package service

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"errors"
)

func Decrypt(data string, key []byte) (string, error) {
	decodedData, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	if len(decodedData) < aes.BlockSize {
		return "", errors.New("invalid data length")
	}

	iv := decodedData[:aes.BlockSize]
	encryptedData := decodedData[aes.BlockSize:]

	mode, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	decryptedData, err := mode.Open(nil, iv, encryptedData, nil)
	if err != nil {
		return "", err
	}

	return string(decryptedData), nil
}
