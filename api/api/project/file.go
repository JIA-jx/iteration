package project

import (
	"context"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"io"
	"net/http"
	"strconv"
	"test.com/api/api/rpc"
	common "test.com/others"
	"test.com/others/errs"
	"test.com/rpcOrgrpc/file"
	"test.com/rpcOrgrpc/fileupload"
	"time"
)

type HandlerFile struct{}

func (f *HandlerFile) upload(c *gin.Context) {
	result := &common.Result{}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	fileHeader, err := c.FormFile("file")
	chunkNumber := c.PostForm("chunkNumber")
	chunkSize := c.PostForm("chunkSize")
	totalChunks := c.PostForm("totalChunks")
	md5 := c.PostForm("md5")

	if err != nil {
		c.JSON(http.StatusOK, result.Fail(400, "获取文件失败"))
		return
	}

	File, err := fileHeader.Open()
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(400, "打开文件失败"))
		return
	}
	defer File.Close()

	fileBytes, err := io.ReadAll(File)
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(500, "读取文件失败"))
		return
	}

	msg := &file.UploadRequest{
		Md5:         md5,
		ChunkNumber: chunkNumber,
		TotalChunks: totalChunks,
		ChunkSize:   chunkSize,
		FileContent: fileBytes,
		FileName:    fileHeader.Filename,
		FileSize:    fileHeader.Size,
	}

	res, err := rpc.FileServiceClient.Upload(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success(res))
}

func (f *HandlerFile) list(c *gin.Context) {
	result := &common.Result{}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	msg := &file.ListRequest{}
	res, err := rpc.FileServiceClient.List(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success(res.List))
}

func (f *HandlerFile) download(c *gin.Context) {
	result := &common.Result{}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()

	id := c.PostForm("id")
	if id == "" {
		c.JSON(http.StatusOK, result.Fail(400, "文件ID不能为空"))
		return
	}

	stream, err := rpc.FileServiceClient.Download(ctx, &file.DownloadRequest{Id: id})
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	resp, err := stream.Recv()
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(500, "下载失败"))
		return
	}
	if resp == nil {
		c.JSON(http.StatusOK, result.Fail(500, "下载失败"))
		return
	}
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Disposition", "attachment; filename="+resp.Name)
	c.Header("Content-Length", strconv.FormatInt(resp.Size, 10))

	for {
		r, rErr := stream.Recv()
		if rErr != nil {
			if rErr != io.EOF {
				zap.L().Warn("download stream interrupted", zap.Error(rErr))
			}
			break
		}
		if _, wErr := c.Writer.Write(r.Content); wErr != nil {
			zap.L().Warn("download write failed", zap.Error(wErr))
			break
		}
	}
}

func (f *HandlerFile) uploadInit(c *gin.Context) {
	result := &common.Result{}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	msg := &fileupload.InitUploadRequest{
		UploadId:    c.PostForm("uploadId"),
		FileName:    c.PostForm("fileName"),
		FileMd5:     c.PostForm("md5"),
		ContentType: c.PostForm("contentType"),
	}
	fileSize, _ := strconv.ParseInt(c.PostForm("fileSize"), 10, 64)
	totalChunks, _ := strconv.ParseInt(c.PostForm("totalChunks"), 10, 32)
	msg.FileSize = fileSize
	msg.TotalChunks = int32(totalChunks)

	if msg.FileName == "" || msg.TotalChunks <= 0 {
		c.JSON(http.StatusOK, result.Fail(400, "fileName 与 totalChunks 不能为空"))
		return
	}
	if msg.FileSize <= 0 {
		c.JSON(http.StatusOK, result.Fail(400, "fileSize 必须为正数"))
		return
	}

	res, err := rpc.ChunkUploadServiceClient.InitUpload(ctx, msg)
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success(res))
}

func (f *HandlerFile) uploadStatus(c *gin.Context) {
	result := &common.Result{}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	uploadId := c.PostForm("uploadId")
	if uploadId == "" {
		c.JSON(http.StatusOK, result.Fail(400, "uploadId 不能为空"))
		return
	}

	res, err := rpc.ChunkUploadServiceClient.UploadStatus(ctx, &fileupload.UploadStatusRequest{UploadId: uploadId})
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success(res))
}

func (f *HandlerFile) uploadChunk(c *gin.Context) {
	result := &common.Result{}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()

	uploadId := c.PostForm("uploadId")
	chunkNumber, _ := strconv.ParseInt(c.PostForm("chunkNumber"), 10, 32)
	if uploadId == "" || chunkNumber <= 0 {
		c.JSON(http.StatusOK, result.Fail(400, "uploadId 与 chunkNumber 不能为空"))
		return
	}

	fileHeader, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(400, "获取分片失败"))
		return
	}

	file, err := fileHeader.Open()
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(400, "打开分片失败"))
		return
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		c.JSON(http.StatusOK, result.Fail(500, "读取分片失败"))
		return
	}

	res, err := rpc.ChunkUploadServiceClient.UploadChunk(ctx, &fileupload.UploadChunkRequest{
		UploadId:    uploadId,
		ChunkMd5:    c.PostForm("chunkMd5"),
		ChunkNumber: int32(chunkNumber),
		FileContent: content,
	})
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success(res))
}

func (f *HandlerFile) uploadMerge(c *gin.Context) {
	result := &common.Result{}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()

	uploadId := c.PostForm("uploadId")
	if uploadId == "" {
		c.JSON(http.StatusOK, result.Fail(400, "uploadId 不能为空"))
		return
	}

	res, err := rpc.ChunkUploadServiceClient.MergeChunks(ctx, &fileupload.MergeChunksRequest{UploadId: uploadId})
	if err != nil {
		code, msg := errs.ParseGrpcError(err)
		c.JSON(http.StatusOK, result.Fail(code, msg))
		return
	}

	c.JSON(http.StatusOK, result.Success(res))
}

func NewFile() *HandlerFile {
	return &HandlerFile{}
}
