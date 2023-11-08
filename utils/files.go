package utils

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
)

type Files struct {
	ListChunkPath []string
}

func NewFiles() *Files {
	return &Files{}
}

func Remove(arg any) {
	switch v := arg.(type) {
	case []string:
		for _, file := range v {
			if err := os.Remove(file); err != nil {
				log.Fatalln(err)
			}
		}
	case map[string]string:
		for _, file := range v {
			if err := os.Remove(file); err != nil {
				log.Fatalln(err)
			}
		}
	}
}

func EnsureFiles(file string) error {
	_, err := os.Stat(file)
	if os.IsNotExist(err) {
		return fmt.Errorf("file `%s` is not Exist.", file)
	}
	return nil
}

func (f *Files) ChunkFile(path string) error {

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	chunkSize := 5000
	chunkIndex := 1
	chunkLines := []string{}

	for scanner.Scan() {
		line := scanner.Text()
		chunkLines = append(chunkLines, line)

		if len(chunkLines) == chunkSize {
			if err := f.saveChunk(chunkLines, chunkIndex); err != nil {
				return err
			}
			chunkIndex++
			chunkLines = []string{}
		}
	}

	if len(chunkLines) > 0 {
		if err := f.saveChunk(chunkLines, chunkIndex); err != nil {
			return err
		}
	}

	return nil
}

func (f *Files) saveChunk(lines []string, index int) error {
	chunkFileName := fmt.Sprintf("%s/chunk_%d.jsonl", BaseChunkPath, index)
	chunkFile, err := os.Create(chunkFileName)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer chunkFile.Close()

	writer := bufio.NewWriter(chunkFile)
	for _, line := range lines {
		fmt.Fprintln(writer, line)
	}

	if err := writer.Flush(); err != nil {
		return err
	}
	f.ListChunkPath = append(f.ListChunkPath, chunkFileName)
	return nil
}

func EnsureDir(name string) error {
	_, err := os.Open(name)
	if errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(name, os.FileMode(0760))
		if err != nil {
			return err
		}
	}
	return nil
}
