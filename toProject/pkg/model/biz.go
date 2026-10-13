package model

import "os"

const (
	Normal         = 0
	Personal int32 = 1
)

var AESKey = resolveKey("qwertyuiopasdfghjkl1234")

func resolveKey(fallback string) string {
	if v := os.Getenv("AES_ID_KEY"); len(v) == 16 || len(v) == 24 || len(v) == 32 {
		return v
	}
	return fallback
}

const (
	NoDeleted = iota
	Deleted
)

const (
	NoArchive = iota
	Archive
)

const (
	Open = iota
	Private
	Custom
)

const (
	Default = "default"
	Simple  = "simple"
)

const (
	NoCollected = iota
	Collected
)

const (
	NoOwner = iota
	Owner
)

const (
	NoExecutor = iota
	Executor
)
const (
	NoCanRead = iota
	CanRead
)

const (
	UnDone = iota
	Done
)

const (
	NoComment = iota
	Comment
)
