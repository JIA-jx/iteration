package min

import (
	"bytes"
	"context"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"strconv"
)

type MinioClient struct {
	C *minio.Client
}

func (c *MinioClient) Put(ctx context.Context,
	bucketName string, fileName string, data []byte, size int64, contentType string) (minio.UploadInfo, error) {

	object, err := c.C.PutObject(
		ctx,
		bucketName,
		fileName,
		bytes.NewBuffer(data),
		size,
		minio.PutObjectOptions{ContentType: contentType},
	)
	return object, err
}

func (c *MinioClient) Compose(ctx context.Context,
	bucketName string, fileName string, totalChunks int) (minio.UploadInfo, error) {

	dst := minio.CopyDestOptions{
		Bucket: bucketName,
		Object: fileName,
	}
	var srcs []minio.CopySrcOptions
	for i := 1; i <= totalChunks; i++ {
		formatInt := strconv.FormatInt(int64(i), 10)
		src := minio.CopySrcOptions{
			Bucket: bucketName,
			Object: fileName + "_" + formatInt,
		}
		srcs = append(srcs, src)
	}
	object, err := c.C.ComposeObject(
		ctx,
		dst,
		srcs...,
	)
	return object, err
}

func (c *MinioClient) ComposeWithPrefix(ctx context.Context, bucketName, objectName, prefix string, totalChunks int32) (minio.UploadInfo, error) {
	dst := minio.CopyDestOptions{
		Bucket: bucketName,
		Object: objectName,
	}
	srcs := make([]minio.CopySrcOptions, 0, totalChunks)
	for i := int32(1); i <= totalChunks; i++ {
		srcs = append(srcs, minio.CopySrcOptions{
			Bucket: bucketName,
			Object: prefix + "_" + strconv.FormatInt(int64(i), 10),
		})
	}
	return c.C.ComposeObject(ctx, dst, srcs...)
}

func (c *MinioClient) Exists(ctx context.Context, bucketName, objectName string) (bool, error) {
	_, err := c.C.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.Code == "NoSuchKey" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *MinioClient) Stat(ctx context.Context, bucketName, objectName string) (minio.ObjectInfo, error) {
	return c.C.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{})
}

func (c *MinioClient) Remove(ctx context.Context, bucketName string, objectNames ...string) error {
	objectsCh := make(chan minio.ObjectInfo, len(objectNames))
	for _, name := range objectNames {
		objectsCh <- minio.ObjectInfo{Key: name}
	}
	close(objectsCh)
	for rmErr := range c.C.RemoveObjects(ctx, bucketName, objectsCh, minio.RemoveObjectsOptions{}) {
		if rmErr.Err != nil {
			return rmErr.Err
		}
	}
	return nil
}

func New(endpoint, accessKey, secretKey string, isSecure bool) (*MinioClient, error) {
	minioClient, err := minio.New(
		endpoint,
		&minio.Options{
			Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
			Secure: isSecure,
		})
	return &MinioClient{C: minioClient}, err
}
