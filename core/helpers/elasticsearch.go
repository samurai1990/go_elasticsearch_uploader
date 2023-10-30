package helpers

import (
	"bgptools/core"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"time"

	"net/http"

	elasticsearch7 "github.com/elastic/go-elasticsearch/v7"
)

type ElasticConfig struct {
	Url         string
	ApiKey      string
	ElasticJson ElasticDocs
}

type ElasticDocs struct {
	AsDescription string `json:"as_description"`
	ASN           int    `json:"asn"`
	CountryCode   string `json:"country_code"`
	Prefix        string `json:"prefix"`
	PrefixVersion int    `json:"Prefix_version"`
	TimeStamp     string `json:"timestamp"`
}

func NewElasticConfig(url, apiKey string) *ElasticConfig {
	return &ElasticConfig{
		Url:    url,
		ApiKey: apiKey,
	}
}

func (e *ElasticConfig) UploadtoElastic() error {

	if reflect.ValueOf(e.ElasticJson).IsZero() {
		return fmt.Errorf("%w,elastic json is empty.", core.ErrEmpty)
	}
	body, err := json.Marshal(e.ElasticJson)
	if err != nil {
		return err
	}
	cfg := elasticsearch7.Config{
		Addresses: []string{
			e.Url,
		},
		APIKey: e.ApiKey,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
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

	res, err := es.Index(
		"bgptools",
		bytes.NewReader(body),
	)

	if err != nil {
		return fmt.Errorf("Error indexing document 1: %s", err.Error())
	}
	fmt.Println("status code: ", res.StatusCode)
	defer res.Body.Close()
	return nil
}
