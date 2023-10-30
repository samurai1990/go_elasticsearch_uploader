package utils

import (
	"fmt"
	"os"
)

func Remove(names map[string]string) {
	for _, file := range names {
		os.Remove(file)
	}
}

func EnsureFiles(file string) error {
	_, err := os.Stat(file)
	if os.IsNotExist(err) {
		return fmt.Errorf("file `%s` is not Exist.", file)
	}
	return nil
}
