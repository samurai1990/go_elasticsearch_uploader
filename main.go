package main

import (
	"bgptools/core/helpers"
	"bgptools/utils"
	"log"
	"os"
)

func main() {

	// log config
	logName := "/var/log/bgp-elastic-uploader/bgp-elastic-uploader.log"
	logFile, err := os.OpenFile(logName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Println(err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	// read config
	conf := utils.NewConfig()
	conf.LoadConfig(".")

	// ensure S3 connection
	s3Info := helpers.NewMinioInfo(conf.MinioIP, conf.MinioPort, conf.MinioAccessKey, conf.MinioSecertKey, "bgptools")
	s3 := helpers.NewStorage(s3Info)
	if err := s3.MinioConnection(); err != nil {
		log.Fatal("not connect to s3 server")
	}

	// get list object from S3
	typeFiles := []string{"table", "asn", "GeoLite2-Country"}
	s3.ListTypeFile = typeFiles
	errS3, objcsS3 := s3.ListObjectS3()
	if errS3 != nil {
		log.Fatal(err)
	}

	// extract last file and get from S3
	if err := s3.LastFileS3(objcsS3); err != nil {
		log.Fatal(err)
	}
	if err := s3.GetS3(); err != nil {
		log.Fatal(err)
	}

	// extract tar.gz file
	tarGZ := utils.NewTAR()
	tarGZ.TarFiles = s3.EnsureFiles
	if err := tarGZ.ExtractTarGz(); err != nil {
		log.Fatalln(err)
	}

	// chunk file
	chunk := utils.NewFiles()
	if err := chunk.ChunkFile(tarGZ.ExtraxtFiles["table"]); err != nil {
		log.Fatal(err)
	}

	// gather data and upload to elastic search
	gather := helpers.NewGatherInfo(chunk.ListChunkPath, tarGZ.ExtraxtFiles["GeoLite2-Country"], tarGZ.ExtraxtFiles["asn"], conf.NumberDeliveries)
	elk := helpers.NewElasticConfig(conf.ElasticUrl, conf.ElasticApikey)
	gather.ElasticInterface = elk
	if err := gather.RunGather(); err != nil {
		log.Fatal(err)
	}

	// remove all file
	log.Println("cleaning...")
	utils.Remove(s3.EnsureFiles)
	utils.Remove(tarGZ.ExtraxtFiles)
	utils.Remove(chunk.ListChunkPath)

	return

}
