package file_service_v1

import (
	"context"
	"fmt"
	lru "github.com/hashicorp/golang-lru"
	"github.com/minio/minio-go/v7"
	"io"
	"strconv"
	"test.com/others/min"
	"test.com/rpcOrgrpc/file"
	"test.com/toProject/internal/dao"
)

var fileDetailLRU, _ = lru.NewARC(10000)
var minioClient, _ = min.New(
	"localhost:9009",
	"",
	"",
	false,
)

type FileService struct {
	file.UnimplementedFileServiceServer
	fileRepo *dao.FileDao
}

func New() *FileService {
	return &FileService{
		fileRepo: dao.NewFileDao(),
	}
}

func (s *FileService) Upload(ctx context.Context, msg *file.UploadRequest) (*file.UploadResponse, error) {
	bucket := "default"
	contentType := "application/octet-stream"

	exist := s.fileRepo.CheckMd5Exist(ctx, msg.Md5)
	if exist {
		if row, fErr := s.fileRepo.FindByMd5(ctx, msg.Md5); fErr == nil && row != nil && row.FilePath != "" {
			url := fmt.Sprintf("http://localhost:9009/%s/%s", bucket, row.FilePath)
			return &file.UploadResponse{
				Url: url,
				Msg: "秒传成功",
			}, nil
		}
	}

	chunkNum, _ := strconv.Atoi(msg.ChunkNumber)
	totalChunk, _ := strconv.Atoi(msg.TotalChunks)

	chunkObjectName := fmt.Sprintf("%s_%d", msg.FileName, chunkNum)
	_, err := minioClient.Put(ctx, bucket, chunkObjectName, msg.FileContent, msg.FileSize, contentType)
	if err != nil {
		return nil, err
	}

	if chunkNum == totalChunk {
		_, err = minioClient.Compose(ctx, bucket, msg.FileName, totalChunk)
		if err != nil {
			return nil, err
		}

		err = s.fileRepo.SaveFileRecord(ctx, msg.FileName, msg.Md5, msg.FileSize)
		if err != nil {
			return nil, err
		}
	}

	url := fmt.Sprintf("http://localhost:9009/%s/%s", bucket, msg.FileName)
	return &file.UploadResponse{
		Url: url,
		Msg: "上传成功",
	}, nil
}

func (s *FileService) List(ctx context.Context, msg *file.ListRequest) (*file.ListResponse, error) {
	cacheKey := "file:list:all"
	if val, ok := fileDetailLRU.Get(cacheKey); ok {
		return val.(*file.ListResponse), nil
	}

	list, err := s.fileRepo.GetFileList(ctx)
	if err != nil {
		return nil, err
	}

	var items []*file.ListItem
	for _, v := range list {
		items = append(items, &file.ListItem{
			Id:        strconv.FormatInt(v.ID, 10),
			Name:      v.Filename,
			Url:       v.FilePath,
			Md5:       v.Md5,
			Size:      v.Size,
			CreatedAt: v.CreatedAt.Format("2006-01-02 15:04:05"),
		})
	}

	resp := &file.ListResponse{
		List: items,
	}

	fileDetailLRU.Add(cacheKey, resp)

	return resp, nil
}

func (s *FileService) Download(req *file.DownloadRequest, stream file.FileService_DownloadServer) error {
	if req.Id == "" {
		return nil
	}
	ctx := stream.Context()

	fileId, _ := strconv.ParseInt(req.Id, 10, 64)
	fileInfo, err := s.fileRepo.GetFileByID(ctx, fileId)
	if err != nil {
		return err
	}

	reader, err := minioClient.C.GetObject(
		ctx,
		"default",
		fileInfo.FilePath,
		minio.GetObjectOptions{},
	)
	if err != nil {
		return err
	}
	defer reader.Close()

	buf := make([]byte, 1024*1024)
	for {
		n, err := reader.Read(buf)
		if n > 0 {

			sendErr := stream.Send(&file.DownloadResponse{
				Content: buf[:n],
				Name:    fileInfo.Filename,
				Size:    fileInfo.Size,
			})
			if sendErr != nil {
				return sendErr
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	return nil
}
