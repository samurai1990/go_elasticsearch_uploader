package helpers

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"net/http"

	elasticsearch7 "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
)

type ElasticConfig struct {
	Url      string
	ApiKey   string
	DocsJson ElasticDocs
	Client   *elasticsearch7.Client
}

type ElasticDocs struct {
	AsDescription string `json:"as_description"`
	ASN           int    `json:"asn"`
	CountryCode   string `json:"country_code"`
	Prefix        string `json:"prefix"`
	PrefixVersion int    `json:"prefix_version"`
	TimeStamp     string `json:"timestamp"`
}

func NewElasticConfig(url, apiKey string) *ElasticConfig {
	return &ElasticConfig{
		Url:    url,
		ApiKey: apiKey,
	}
}

func (e *ElasticConfig) Connect() error {
	cfg := elasticsearch7.Config{
		Addresses: []string{
			e.Url,
		},
		APIKey: e.ApiKey,
		Transport: &http.Transport{
			ExpectContinueTimeout: time.Second * 3,
			TLSHandshakeTimeout:   time.Second * 3,
			MaxIdleConnsPerHost:   2,
			ResponseHeaderTimeout: time.Second * 3,
			DialContext:           (&net.Dialer{Timeout: time.Second * 3}).DialContext,
			TLSClientConfig: &tls.Config{
				MinVersion:         tls.VersionTLS13,
				InsecureSkipVerify: true,
			},
		},
	}
	es, err := elasticsearch7.NewClient(cfg)

	if err != nil {
		return fmt.Errorf("Elasticsearch connection error: %s", err.Error())
	}

	e.Client = es
	return nil

}

func (es *ElasticConfig) UploadBlunktoElastic(docsList []*ElasticDocs) ([]*ElasticDocs, error) {

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         "bgptools",
		Client:        es.Client,
		NumWorkers:    10,
		FlushBytes:    5e+6,
		FlushInterval: 30 * time.Second,
	})

	if err != nil {
		log.Fatalf("Error creating the indexer: %s", err)
	}

	var FailList []*ElasticDocs
	for _, doc := range docsList {
		data, err := json.Marshal(doc)
		if err != nil {
			log.Fatalf("Error marshalling document: %s", err)
		}
		err = bi.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				Action: "index",
				Body:   strings.NewReader(string(data)),
				OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
					log.Printf("doc: %v | status : %d", item.Body, res.Status)
					atomic.AddInt64(&COUNT, 1)
				},
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					log.Printf("doc: %v | status : %d", item.Body, res.Status)
					FailList = append(FailList, doc)
				},
			},
		)
		if err != nil {
			log.Fatalf("Error adding document to indexer: %s", err)
		}
	}

	if err := bi.Close(context.Background()); err != nil {
		log.Fatalf("Error closing the indexer: %s", err)
	}

	status := "successed"
	if len(FailList) != 0 {
		status = "fail"
	}

	log.Printf("Added %d :: Failed %d :: status %s", bi.Stats().NumAdded, bi.Stats().NumFailed, status)
	return FailList, nil
}
