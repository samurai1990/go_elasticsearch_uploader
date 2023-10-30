package utils

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

type TARGz struct {
	TarFiles     map[string]string
	ExtraxtFiles map[string]string
}

func NewTAR() *TARGz {
	return &TARGz{}
}

func (t *TARGz) ExtractTarGz() error {
	wg := sync.WaitGroup{}
	mux := sync.Mutex{}
	newEntries := make(map[string]string)

	for f, filename := range t.TarFiles {
		wg.Add(1)
		go func(format, file string, e map[string]string) {
			tgz, err := os.Open(file)
			if err != nil {
				log.Fatalf("error writing archive: %s", err.Error())
			}
			defer tgz.Close()

			uncompressedStream, err := gzip.NewReader(tgz)
			if err != nil {
				log.Fatalln(err)
			}

			tarReader := tar.NewReader(uncompressedStream)

			for {
				header, err := tarReader.Next()
				if err == io.EOF {
					break
				}

				if err != nil {
					log.Fatalf("ExtractTarGz: Next() failed: %s", err.Error())
				}

				absolutePath := fmt.Sprintf("%s/%s", TempPath, header.Name)
				switch header.Typeflag {

				case tar.TypeDir:
					if err := os.Mkdir(absolutePath, 0755); err != nil {
						log.Fatalf("ExtractTarGz: Mkdir() failed: %s", err.Error())
					}
				case tar.TypeReg:
					outFile, err := os.Create(absolutePath)
					if err != nil {
						log.Fatalf("ExtractTarGz: Create() failed: %s", err.Error())
					}
					if _, err := io.Copy(outFile, tarReader); err != nil {
						log.Fatalf("ExtractTarGz: Copy() failed: %s", err.Error())
					}
					outFile.Close()
					mux.Lock()
					e[format] = absolutePath
					mux.Unlock()
					wg.Done()

				default:
					log.Fatalf("ExtractTarGz: uknown type: %v in %s", header.Typeflag, header.Name)
				}

			}
		}(f, filename, newEntries)
	}

	wg.Wait()

	for _, filePath := range newEntries {
		if err := EnsureFiles(filePath); err != nil {
			return err
		}
	}

	t.ExtraxtFiles = newEntries

	return nil
}
