package upload_service_v1

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"go.uber.org/zap"
	"test.com/others/encrypts"
	"test.com/others/errs"
	"test.com/others/min"
	"test.com/others/tenant"
	"test.com/rpcOrgrpc/fileupload"
	"test.com/toProject/internal/dao"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/repo"
	"test.com/toProject/pkg/model"
)

const (
	defaultBucket     = "default"
	sessionTTL        = 24 * time.Hour
	chunkSuffixFormat = "%s_%d"
)

var (
	uploadLRU, _ = lru.NewARC(1000)

	minioClient, _ = min.New(
		"localhost:9009",
		"",
		"",
		false,
	)
)

type UploadService struct {
	fileupload.UnimplementedChunkUploadServiceServer
	cache    repo.Cache
	fileRepo repo.FileRepo
}

func New() *UploadService {
	return &UploadService{
		cache:    dao.Rc,
		fileRepo: dao.NewFileDao(),
	}
}

func sessionKey(uploadId string) string {
	return "upload:session:" + uploadId
}

func chunkSetKey(uploadId string) string {
	return "upload:chunks:" + uploadId
}

func (s *UploadService) loadSession(ctx context.Context, uploadId string) (*data.UploadSession, error) {
	if uploadId == "" {
		return nil, nil
	}

	raw, err := s.cache.Get(ctx, sessionKey(uploadId))
	if err != nil || raw == "" {
		return nil, err
	}

	session := &data.UploadSession{}
	if err := json.Unmarshal([]byte(raw), session); err != nil {
		return nil, err
	}
	return session, nil
}

func (s *UploadService) uploadedChunks(ctx context.Context, uploadId string) ([]int32, error) {
	members, err := s.cache.SMembers(ctx, chunkSetKey(uploadId))
	if err != nil {
		return nil, err
	}

	chunks := make([]int32, 0, len(members))
	for _, m := range members {
		n, convErr := strconv.Atoi(m)
		if convErr != nil {
			continue
		}
		chunks = append(chunks, int32(n))
	}
	return chunks, nil
}

const maxTotalChunks = 100000

func missingChunks(uploaded []int32, total int32) []int32 {
	seen := make(map[int32]struct{}, len(uploaded))
	for _, n := range uploaded {
		seen[n] = struct{}{}
	}

	missing := make([]int32, 0, total)
	for i := int32(1); i <= total; i++ {
		if _, ok := seen[i]; !ok {
			missing = append(missing, i)
		}
	}
	return missing
}

func (s *UploadService) InitUpload(ctx context.Context, req *fileupload.InitUploadRequest) (*fileupload.InitUploadResponse, error) {
	if req.FileName == "" {
		return nil, errs.GrpcError(model.ParamsError)
	}
	if req.TotalChunks <= 0 || req.TotalChunks > maxTotalChunks {
		return nil, errs.GrpcError(model.ParamsError)
	}

	if t, tErr := dao.FindTenantById(ctx, tenant.IdFromContext(ctx)); tErr != nil {
		return nil, errs.GrpcError(model.DBError)
	} else if t != nil && t.StorageQuotaMb > 0 && req.FileSize > 0 {
		used, sErr := s.fileRepo.SumSizeByTenant(ctx)
		if sErr != nil {
			return nil, errs.GrpcError(model.DBError)
		}
		if used+req.FileSize > t.StorageQuotaMb*1024*1024 {
			return nil, errs.GrpcError(errs.NewError(403002, "租户存储配额已用尽"))
		}
	}

	bucket := defaultBucket
	contentType := req.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	uploadId := req.UploadId
	if uploadId == "" {
		uploadId = encrypts.Md5(fmt.Sprintf("%s:%d:%d", req.FileName, req.FileSize, time.Now().UnixNano()))
	}

	if req.FileMd5 != "" && s.fileRepo.CheckMd5Exist(ctx, req.FileMd5) {
		url := s.objectURL(bucket, req.FileName)
		uploadLRU.Add(uploadId, url)
		return &fileupload.InitUploadResponse{
			UploadId:    uploadId,
			TotalChunks: req.TotalChunks,
			Finished:    true,
			Url:         url,
			Instant:     true,
		}, nil
	}

	session := &data.UploadSession{
		UploadId:    uploadId,
		FileName:    req.FileName,
		ObjectName:  req.FileName,
		ChunkPrefix: fmt.Sprintf("chunks/%s/%s", uploadId, req.FileName),
		FileSize:    req.FileSize,
		TotalChunks: req.TotalChunks,
		FileMd5:     req.FileMd5,
		ContentType: contentType,
		Bucket:      bucket,
		CreatedAt:   time.Now().UnixMilli(),
	}

	body, err := json.Marshal(session)
	if err != nil {
		return nil, errs.GrpcError(model.BusinessError)
	}
	if err := s.cache.Put(ctx, sessionKey(uploadId), string(body), sessionTTL); err != nil {
		return nil, errs.GrpcError(model.RedisError)
	}

	uploaded, err := s.uploadedChunks(ctx, uploadId)
	if err != nil {
		zap.L().Warn("list uploaded chunks failed", zap.String("upload_id", uploadId), zap.Error(err))
	}

	zap.L().Info("upload session inited",
		zap.String("upload_id", uploadId),
		zap.String("file_name", req.FileName),
		zap.Int32("total_chunks", req.TotalChunks),
		zap.Int("resumed_chunks", len(uploaded)))

	return &fileupload.InitUploadResponse{
		UploadId:       uploadId,
		UploadedChunks: uploaded,
		TotalChunks:    req.TotalChunks,
		Finished:       false,
	}, nil
}

func (s *UploadService) UploadStatus(ctx context.Context, req *fileupload.UploadStatusRequest) (*fileupload.UploadStatusResponse, error) {
	session, err := s.loadSession(ctx, req.UploadId)
	if err != nil {
		return nil, errs.GrpcError(model.RedisError)
	}
	if session == nil {
		return nil, errs.GrpcError(model.NotFoundError)
	}

	uploaded, err := s.uploadedChunks(ctx, req.UploadId)
	if err != nil {
		return nil, errs.GrpcError(model.RedisError)
	}

	resp := &fileupload.UploadStatusResponse{
		UploadId:       req.UploadId,
		UploadedChunks: uploaded,
		MissingChunks:  missingChunks(uploaded, session.TotalChunks),
		TotalChunks:    session.TotalChunks,
	}

	if url, ok := uploadLRU.Get(req.UploadId); ok {
		resp.Finished = true
		resp.Url, _ = url.(string)
	}
	return resp, nil
}

func (s *UploadService) UploadChunk(ctx context.Context, req *fileupload.UploadChunkRequest) (*fileupload.UploadChunkResponse, error) {
	if req.UploadId == "" {
		return nil, errs.GrpcError(model.ParamsError)
	}
	if req.ChunkNumber <= 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}
	if len(req.FileContent) == 0 {
		return nil, errs.GrpcError(model.ParamsError)
	}

	session, err := s.loadSession(ctx, req.UploadId)
	if err != nil {
		return nil, errs.GrpcError(model.RedisError)
	}
	if session == nil {
		return nil, errs.GrpcError(model.NotFoundError)
	}
	if req.ChunkNumber > session.TotalChunks {
		return nil, errs.GrpcError(model.ParamsError)
	}

	chunkObject := fmt.Sprintf(chunkSuffixFormat, session.ChunkPrefix, req.ChunkNumber)
	_, err = minioClient.Put(ctx, session.Bucket, chunkObject, req.FileContent, int64(len(req.FileContent)), session.ContentType)
	if err != nil {
		zap.L().Error("put chunk failed",
			zap.String("upload_id", req.UploadId),
			zap.Int32("chunk", req.ChunkNumber),
			zap.Error(err))
		return nil, errs.GrpcError(model.UploadChunkFail)
	}

	if _, err := s.cache.SAdd(ctx, chunkSetKey(req.UploadId), strconv.Itoa(int(req.ChunkNumber))); err != nil {
		zap.L().Warn("record chunk failed", zap.String("upload_id", req.UploadId), zap.Error(err))
	}
	if _, err := s.cache.SetNX(ctx, chunkSetKey(req.UploadId), "1", sessionTTL); err != nil {
		zap.L().Warn("renew chunk set ttl failed", zap.String("upload_id", req.UploadId), zap.Error(err))
	}

	resp := &fileupload.UploadChunkResponse{
		UploadId:    req.UploadId,
		ChunkNumber: req.ChunkNumber,
		TotalChunks: session.TotalChunks,
	}

	uploaded, err := s.uploadedChunks(ctx, req.UploadId)
	if err != nil {
		return nil, errs.GrpcError(model.RedisError)
	}
	resp.ReceivedChunks = int32(len(uploaded))

	if len(uploaded) >= int(session.TotalChunks) {
		url, mergeErr := s.merge(ctx, session)
		if mergeErr != nil {
			return nil, mergeErr
		}
		resp.Finished = true
		resp.Url = url
	}

	return resp, nil
}

func (s *UploadService) MergeChunks(ctx context.Context, req *fileupload.MergeChunksRequest) (*fileupload.MergeChunksResponse, error) {
	session, err := s.loadSession(ctx, req.UploadId)
	if err != nil {
		return nil, errs.GrpcError(model.RedisError)
	}
	if session == nil {
		return nil, errs.GrpcError(model.NotFoundError)
	}

	uploaded, err := s.uploadedChunks(ctx, req.UploadId)
	if err != nil {
		return nil, errs.GrpcError(model.RedisError)
	}
	if missing := missingChunks(uploaded, session.TotalChunks); len(missing) > 0 {
		zap.L().Warn("merge rejected, chunks missing",
			zap.String("upload_id", req.UploadId),
			zap.Int("missing", len(missing)))
		return nil, errs.GrpcError(model.ChunkMissing)
	}

	url, err := s.merge(ctx, session)
	if err != nil {
		return nil, err
	}

	return &fileupload.MergeChunksResponse{
		Url:      url,
		FileId:   encrypts.Md5(session.ObjectName),
		FileSize: session.FileSize,
	}, nil
}

func (s *UploadService) merge(ctx context.Context, session *data.UploadSession) (string, error) {
	if url, ok := uploadLRU.Get(session.UploadId); ok {
		if cachedURL, ok := url.(string); ok {
			return cachedURL, nil
		}
	}

	_, err := minioClient.ComposeWithPrefix(ctx, session.Bucket, session.ObjectName, session.ChunkPrefix, session.TotalChunks)
	if err != nil {
		zap.L().Error("compose chunks failed",
			zap.String("upload_id", session.UploadId),
			zap.Int32("total_chunks", session.TotalChunks),
			zap.Error(err))
		return "", errs.GrpcError(model.BusinessError)
	}

	fileSize := session.FileSize
	if fileSize <= 0 {
		if info, statErr := minioClient.Stat(ctx, session.Bucket, session.ObjectName); statErr == nil {
			fileSize = info.Size
		}
	}
	if err := s.fileRepo.SaveFileRecord(ctx, session.ObjectName, session.FileMd5, fileSize); err != nil {
		zap.L().Warn("save file record failed", zap.String("object", session.ObjectName), zap.Error(err))
	}

	url := s.objectURL(session.Bucket, session.ObjectName)
	uploadLRU.Add(session.UploadId, url)

	go s.cleanupChunks(session)

	zap.L().Info("upload merged",
		zap.String("upload_id", session.UploadId),
		zap.String("object", session.ObjectName),
		zap.Int64("size", fileSize))

	return url, nil
}

func (s *UploadService) cleanupChunks(session *data.UploadSession) {
	defer func() {
		if r := recover(); r != nil {
			zap.L().Error("cleanup chunks panic", zap.Any("r", r))
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	objects := make([]string, 0, session.TotalChunks)
	for i := int32(1); i <= session.TotalChunks; i++ {
		objects = append(objects, fmt.Sprintf(chunkSuffixFormat, session.ChunkPrefix, i))
	}

	if err := minioClient.Remove(ctx, session.Bucket, objects...); err != nil {
		zap.L().Warn("cleanup chunk objects failed", zap.String("upload_id", session.UploadId), zap.Error(err))
	}
	if _, err := s.cache.Del(ctx, chunkSetKey(session.UploadId)); err != nil {
		zap.L().Warn("cleanup chunk set failed", zap.String("upload_id", session.UploadId), zap.Error(err))
	}
}

func (s *UploadService) objectURL(bucket, objectName string) string {
	if strings.Contains(objectName, "/") {
		objectName = filepath.Base(objectName)
	}
	return fmt.Sprintf("http://localhost:9009/%s/%s", bucket, objectName)
}
