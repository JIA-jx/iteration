package secretfile

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var sharedKeyCandidates = []string{
	"conf/rsa/rsa_private_key.pem",
	"../api/api/midd/conf/rsa/rsa_private_key.pem",
	"api/api/midd/conf/rsa/rsa_private_key.pem",
	"api/midd/conf/rsa/rsa_private_key.pem",
	"../conf/rsa/rsa_private_key.pem",
}

func deriveFromSharedKey(size int) string {
	for _, candidate := range sharedKeyCandidates {
		data, err := os.ReadFile(candidate)
		if err != nil || len(data) == 0 {
			continue
		}
		sum := sha256.Sum256(append([]byte("iteration-secret:"), data...))
		return hex.EncodeToString(sum[:size])
	}
	return ""
}

func Load(envName string, fileName string, size int) string {
	if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
		return v
	}

	path := filepath.Join(os.TempDir(), fileName)
	if b, err := os.ReadFile(path); err == nil {
		if v := strings.TrimSpace(string(b)); v != "" {
			return v
		}
	}

	if v := deriveFromSharedKey(size); v != "" {
		return v
	}

	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		panic("generate " + envName + " failed: " + err.Error())
	}
	val := hex.EncodeToString(buf)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if b, rErr := os.ReadFile(path); rErr == nil {
			if v := strings.TrimSpace(string(b)); v != "" {
				return v
			}
		}
		log.Printf("persist %s to %s failed: %v", envName, path, err)
		return val
	}
	defer f.Close()
	if _, err := f.WriteString(val); err != nil {
		log.Printf("write %s to %s failed: %v", envName, path, err)
	}
	log.Printf("%s not set, generated host-local secret at %s; set %s explicitly for multi-host deployments", envName, path, envName)
	return val
}
