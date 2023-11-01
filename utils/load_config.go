package utils

import (
	"log"

	"github.com/spf13/viper"
)

const (
	TempPath      = "/tmp/bgptools_elk"
	BaseChunkPath = TempPath + "/chunks"
)

type config struct {
	MinioIP          string `mapstructure:"MINIO_ENDPOINT_IP"`
	MinioPort        string `mapstructure:"MINIO_ENDPOINT_PORT"`
	MinioAccessKey   string `mapstructure:"MINIO_ACCESS_KEY"`
	MinioSecertKey   string `mapstructure:"MINIO_SECRET_KEY"`
	ElasticUrl       string `mapstructure:"ELASTIC_URL"`
	ElasticApikey    string `mapstructure:"ELASTIC_APIKEY"`
	NumberDeliveries int    `mapstructure:"NUMBER_DELIVERIES"`
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

	if err := EnsureDir(TempPath); err != nil {
		return err
	}

	if err := EnsureDir(BaseChunkPath); err != nil {
		return err
	}

	return nil
}
