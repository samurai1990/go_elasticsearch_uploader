package helpers

import (
	"os"

	"github.com/rosedblabs/rosedb/v2"
)

type CacheDB struct {
	PathDB string
	DB     *rosedb.DB
}

func NewCacheDB(path string) *CacheDB {
	return &CacheDB{
		PathDB: path,
	}
}

func (c *CacheDB) HandleCacheDB() error {
	options := rosedb.DefaultOptions
	options.DirPath = c.PathDB

	// open a database
	db, err := rosedb.Open(options)
	if err != nil {
		return err
	}
	c.DB = db
	return nil
}

func (c *CacheDB) Set(key, value string) error {
	if err := c.DB.Put([]byte(key), []byte(value)); err != nil {
		return err
	}
	return nil
}

func (c *CacheDB) Get(key string) (string, error) {
	value, err := c.DB.Get([]byte(key))
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (c *CacheDB) DropDB() error {
	if err := os.RemoveAll(c.PathDB); err != nil {
		return err
	}
	return nil
}
