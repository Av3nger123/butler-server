package internals

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
)

func Decrypt(encryptedData string, key []byte, iv, tag string) ([]byte, error) {
	b64EncryptedData, err := base64.StdEncoding.DecodeString(encryptedData)
	if err != nil {
		return nil, fmt.Errorf("error decoding base64 encrypted data: %w", err)
	}

	b64Tag, err := base64.StdEncoding.DecodeString(tag)
	if err != nil {
		return nil, fmt.Errorf("error decoding base64 tag: %w", err)
	}

	b64Iv, err := base64.StdEncoding.DecodeString(iv)
	if err != nil {
		return nil, fmt.Errorf("error decoding base64 tag: %w", err)
	}

	// Parse key and IV
	parsedKey, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("error creating AES cipher: %w", err)
	}

	aesGCM, err := cipher.NewGCM(parsedKey)
	if err != nil {
		return nil, fmt.Errorf("error creating GCM cipher: %w", err)
	}
	decrypted, err := aesGCM.Open(nil, b64Iv, b64EncryptedData, b64Tag)
	if err != nil {
		return nil, fmt.Errorf("decryption failed: %w", err)
	}

	fmt.Printf("Decrypted data: %s\n", string(decrypted)) // Log decrypted data
	return decrypted, nil
}
