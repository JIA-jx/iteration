package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
)

func main() {
	privateKey, _ := rsa.GenerateKey(rand.Reader, 2048)

	privFile, _ := os.Create("rsa_private_key.pem")
	pem.Encode(privFile, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})
	privFile.Close()

	pubFile, _ := os.Create("rsa_public_key.pem")
	pem.Encode(pubFile, &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: x509.MarshalPKCS1PublicKey(&privateKey.PublicKey),
	})
	pubFile.Close()
}
