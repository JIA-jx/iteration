package data

type UploadSession struct {
	UploadId    string `json:"upload_id"`
	FileName    string `json:"file_name"`
	ObjectName  string `json:"object_name"`
	ChunkPrefix string `json:"chunk_prefix"`
	FileSize    int64  `json:"file_size"`
	TotalChunks int32  `json:"total_chunks"`
	FileMd5     string `json:"file_md5"`
	ContentType string `json:"content_type"`
	Bucket      string `json:"bucket"`
	CreatedAt   int64  `json:"created_at"`
}
