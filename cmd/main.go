package main

import (
	"context"
	"fmt"

	"github.com/xige-16/storage-test/cmd/minio"
	"github.com/xige-16/storage-test/internal/storage"
	"github.com/xige-16/storage-test/pkg/util/paramtable"
	"sync"
	"time"
)

//type ObjectStorage interface {
//	GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64) (FileReader, error)
//	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) error
//	StatObject(ctx context.Context, bucketName, objectName string) (int64, error)
//	ListObjects(ctx context.Context, bucketName string, prefix string, recursive bool) ([]string, []time.Time, error)
//	RemoveObject(ctx context.Context, bucketName, objectName string) error
//}

func main() {
	ctx := context.Background()
	paramtable.Init()
	Params := paramtable.Get()
	factory := storage.NewChunkManagerFactoryWithParam(Params)
	chunkManager, err := factory.NewPersistentStorageChunkManager(ctx)
	if err != nil {
		panic("init chunk manager failed!")
	}

	remoteChunkManager := chunkManager.(*storage.RemoteChunkManager)
	minioClient := remoteChunkManager.Client
	bucketName := remoteChunkManager.BucketName
	rootPath := remoteChunkManager.RootPath()

	timeout := 300 * time.Second
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		childCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		minio.PutObject(childCtx, minioClient, bucketName, rootPath, 1)
		wg.Done()
	}()

	wg.Wait()

	//offset := 0
	//input := make([]byte, 8000000, 8000000)
	//err = minioClient.PutObject(ctx, bucketName, path.Join(rootPath, common.SegmentInsertLogPath, strconv.Itoa(offset)), bytes.NewReader(input), int64(len(input)))
	//if err != nil {
	//	panic(err)
	//}

	//size, err := minioClient.StatObject(ctx, bucketName, "test1")
	//if err != nil {
	//	panic(err)
	//}
	//
	//object, err := minioClient.GetObject(ctx, bucketName, "test1", int64(0), int64(0))
	//if err != nil {
	//	panic(err)
	//}
	//
	//output, err := storage.Read(object, size)
	//if err != nil {
	//	panic(err)
	//}
	//if len(output) != len(input) {
	//	log.Info("input and output len", zap.Int("input len", len(input)), zap.Int("output len", len(output)))
	//	panic("input and output not consistent")
	//}
	//
	//keys, _, err := minioClient.ListObjects(ctx, bucketName, "", true)
	//if err != nil {
	//	panic(err)
	//}
	//log.Info("list keys", zap.Strings("keys", keys), zap.String("path", rootPath))
	//if !slices.Contains(keys, "test") {
	//	panic("file not exit in list keys")
	//}
	//
	//err = minioClient.RemoveObject(ctx, bucketName, "test")
	//if err != nil {
	//	panic(err)
	//}

	fmt.Print("minio func test done!")
}
