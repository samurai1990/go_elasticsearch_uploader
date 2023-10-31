package utils

import (
	"errors"
	"log"
	"os"

	"github.com/spf13/viper"
)

const (
	TempPath      = "/tmp/bgptools_elk"
	BaseChunkPath = TempPath + "/chunks"
)

type config struct {
	MinioIP        string `mapstructure:"MINIO_ENDPOINT_IP"`
	MinioPort      string `mapstructure:"MINIO_ENDPOINT_PORT"`
	MinioAccessKey string `mapstructure:"MINIO_ACCESS_KEY"`
	MinioSecertKey string `mapstructure:"MINIO_SECRET_KEY"`
	ElasticUrl     string `mapstructure:"ELASTIC_URL"`
	ElasticApikey  string `mapstructure:"ELASTIC_APIKEY"`
}

func NewConfig() *config {
	return &config{}
}

func (c *config) LoadConfig(path string) error {
	viper.AddConfigPath(path)
	viper.SetConfigName("app")
	viper.SetConfigType("env")

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		log.Fatal(err)
	}
	if err := viper.Unmarshal(&c); err != nil {
		log.Fatal(err)
	}

	if err := ensureDir(TempPath); err != nil {
		return err
	}

	if err := ensureDir(BaseChunkPath); err != nil {
		return err
	}

	if err := SetULimit(); err != nil {
		return err
	}

	return nil
}

func ensureDir(name string) error {
	_, err := os.Open(name)
	if errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(name, 0755)
		if err != nil {
			return err
		}
	}
	return nil
}
