package internals

import (
	"crypto/aes"
)

func EncryptAES(text []byte, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	cipherText := make([]byte, len(text))
	block.Encrypt(cipherText, text)

	return cipherText, nil
}

func DecryptAES(cipherText []byte, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	plaintext := make([]byte, len(cipherText))
	block.Decrypt(plaintext, cipherText)

	return plaintext, nil
}
