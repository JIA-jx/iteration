package encrypts

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"test.com/others/secretfile"
)

func Md5(str string) string {
	hash := md5.New()
	_, _ = io.WriteString(hash, str)
	return hex.EncodeToString(hash.Sum(nil))
}

func HashPassword(plain string, salt string) string {
	return Md5(salt + "::" + plain + "::iteration")
}

var commonIV = []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}

var AESKey = resolveKey()

func resolveKey() string {
	if v := os.Getenv("AES_ID_KEY"); len(v) == 16 || len(v) == 24 || len(v) == 32 {
		return v
	}
	if os.Getenv("AES_ID_ALLOW_INSECURE_KEY") == "1" {
		return "sdfgyrhgbxcdgryfhgywertd"
	}
	return secretfile.Load("AES_ID_KEY", ".iteration_aes_id_key", 16)
}

func DecryptNoErr(cipherStr string) int64 {
	decrypt, _ := Decrypt(cipherStr, AESKey)
	parseInt, _ := strconv.ParseInt(decrypt, 10, 64)
	return parseInt
}

const RandomIVPrefix = "v2:"

func randomIVEnabled() bool {
	return os.Getenv("AES_ID_LEGACY_IV") != "1"
}

func Decrypt(cipherStr string, keyText string) (plainText string, err error) {
	keyByte := []byte(keyText)
	c, err := aes.NewCipher(keyByte)
	if err != nil {
		return "", err
	}

	if strings.HasPrefix(cipherStr, RandomIVPrefix) {
		raw, dErr := hex.DecodeString(strings.TrimPrefix(cipherStr, RandomIVPrefix))
		if dErr != nil {
			return "", dErr
		}
		if len(raw) <= aes.BlockSize {
			return "", errors.New("密文长度不足")
		}
		iv := raw[:aes.BlockSize]
		cipherByte := raw[aes.BlockSize:]
		plainByte := make([]byte, len(cipherByte))
		cipher.NewCFBDecrypter(c, iv).XORKeyStream(plainByte, cipherByte)
		return string(plainByte), nil
	}

	cfbdec := cipher.NewCFBDecrypter(c, commonIV)
	cipherByte, _ := hex.DecodeString(cipherStr)
	plainByte := make([]byte, len(cipherByte))
	cfbdec.XORKeyStream(plainByte, cipherByte)
	plainText = string(plainByte)
	return
}
func EncryptNoErr(id int64) string {
	str, _ := EncryptInt64(id, AESKey)
	return str
}
func EncryptInt64(id int64, keyText string) (cipherStr string, err error) {
	idStr := strconv.FormatInt(id, 10)
	return Encrypt(idStr, keyText)
}
func Encrypt(plainText string, keyText string) (cipherStr string, err error) {
	plainByte := []byte(plainText)
	keyByte := []byte(keyText)

	c, err := aes.NewCipher(keyByte)
	if err != nil {
		return "", err
	}

	if randomIVEnabled() {
		iv := make([]byte, aes.BlockSize)
		if _, rErr := rand.Read(iv); rErr != nil {
			return "", rErr
		}
		cipherByte := make([]byte, len(plainByte))
		cipher.NewCFBEncrypter(c, iv).XORKeyStream(cipherByte, plainByte)
		return RandomIVPrefix + hex.EncodeToString(append(iv, cipherByte...)), nil
	}

	cfb := cipher.NewCFBEncrypter(c, commonIV)
	cipherByte := make([]byte, len(plainByte))
	cfb.XORKeyStream(cipherByte, plainByte)
	cipherStr = hex.EncodeToString(cipherByte)
	return
}
