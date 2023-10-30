package helpers

import (
	"bgptools/utils"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinioInfo struct {
	ip              string
	port            string
	accessKeyID     string
	secretAccessKey string
	bucketName      string
}

type Storage struct {
	MinioInfo    MinioInfo
	MinioClient  *minio.Client
	ListFiles    map[string]string
	ListTypeFile []string
	EnsureFiles  map[string]string
}

func NewMinioInfo(ip, port, accessKeyID, secretAccessKey, bucketName string) *MinioInfo {
	return &MinioInfo{
		ip:              ip,
		port:            port,
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
		bucketName:      bucketName,
	}
}

func NewStorage(info *MinioInfo) *Storage {
	return &Storage{
		MinioInfo: *info,
	}
}

func (m *Storage) MinioConnection() error {
	endpoint := m.MinioInfo.ip + ":" + m.MinioInfo.port
	useSSL := false
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(m.MinioInfo.accessKeyID, m.MinioInfo.secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Println(err)
		return err
	}
	m.MinioClient = minioClient
	if err := m.checkBucket(); err != nil {
		log.Fatal(err)
	}
	return nil
}

func (m *Storage) checkBucket() error {
	if ok, err := m.MinioClient.BucketExists(context.Background(), m.MinioInfo.bucketName); err != nil {
		return errors.New(fmt.Sprintf("edge error: %s", err.Error()))
	} else {
		if !ok {
			if err := m.MinioClient.MakeBucket(context.Background(), m.MinioInfo.bucketName, minio.MakeBucketOptions{}); err != nil {
				return errors.New(fmt.Sprintf("make bucket `%s` in edge error: %s", m.MinioInfo.bucketName, err.Error()))
			}
		}
	}
	return nil
}

func (m *Storage) UploadToS3(path string) error {
	filename := strings.Split(path, "/")
	fileObject, err := os.Open(path)
	if err != nil {
		return errors.New(err.Error())
	}
	defer fileObject.Close()

	objectInfo, err := fileObject.Stat()
	if err != nil {
		return errors.New(err.Error())
	}
	_, errU := m.MinioClient.PutObject(context.Background(), m.MinioInfo.bucketName, filename[len(filename)-1], fileObject, objectInfo.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if errU != nil {
		return errors.New(err.Error())
	}
	log.Println(fmt.Sprintf("upload file '%s' successed.", filename[len(filename)-1]))
	os.Remove(path)
	return nil
}

func (m *Storage) ListObjectS3() (error, []minio.ObjectInfo) {
	var objects []minio.ObjectInfo
	for objs := range m.MinioClient.ListObjects(context.Background(), m.MinioInfo.bucketName, minio.ListObjectsOptions{}) {
		if objs.Err != nil {
			return objs.Err, nil
		}
		objects = append(objects, objs)
	}
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].LastModified.After(objects[j].LastModified)
	})

	return nil, objects
}

func (m *Storage) LastFileS3(objects []minio.ObjectInfo) error {
	listFile := make(map[string]string)

	for _, t := range m.ListTypeFile {
		idx := slices.IndexFunc(objects, func(o minio.ObjectInfo) bool { return strings.Contains(o.Key[:20], t) })
		if idx != -1 {
			listFile[t] = objects[idx].Key
		}
	}

	for _, t := range m.ListTypeFile {
		_, ok := listFile[t]
		if !ok {
			return fmt.Errorf("file %s is not exist in s3.", t)
		}
	}
	m.ListFiles = listFile

	return nil
}

func (m *Storage) GetS3() error {
	wg := sync.WaitGroup{}
	mux := sync.Mutex{}
	newEntries := make(map[string]string)

	for f, filename := range m.ListFiles {
		wg.Add(1)
		go func(format, name string, e map[string]string) {
			Newfilepath := fmt.Sprintf("%s/%s", utils.TempPath, name)
			if err := m.MinioClient.FGetObject(context.Background(), m.MinioInfo.bucketName, name, Newfilepath, minio.GetObjectOptions{}); err != nil {
				log.Fatalln(err)
				wg.Done()
			}
			log.Println(fmt.Sprintf("get file '%s' form S3 is successed.", name))
			mux.Lock()
			e[format] = Newfilepath
			mux.Unlock()
			wg.Done()
		}(f, filename, newEntries)

	}
	wg.Wait()

	for _, filePath := range newEntries {
		if err := utils.EnsureFiles(filePath); err != nil {
			return err
		}
	}

	m.EnsureFiles = newEntries

	return nil
}
