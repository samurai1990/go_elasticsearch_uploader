package helpers

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"bgptools/core"
	"bgptools/utils"
)

const (
	BulkNumber int = 5000
)

var COUNT int64

type GatherInfo struct {
	ChunkPath        []string
	GeommdbPath      string
	CsvPath          string
	NumberDelivereis int
	ElasticInterface *ElasticConfig
	cacheDB          *CacheDB
}

type JsonlField struct {
	Cidr string `json:"CIDR"`
	Asn  int    `json:"ASN"`
	Hits int    `json:"Hits"`
}

type CSVField struct {
	asn   string
	name  string
	class string
}

type GatherDelivery struct {
	Docs       []*ElasticDocs
	MaxRetries int `default:"0"`
}

type GatherProsses struct {
	GeommdbPath string
	doc         ElasticDocs
}

func NewGatherInfo(chunkPath []string, geoMMdbPath, CsvPath string, nDeliveries int) *GatherInfo {
	return &GatherInfo{
		ChunkPath:        chunkPath,
		GeommdbPath:      geoMMdbPath,
		CsvPath:          CsvPath,
		NumberDelivereis: nDeliveries,
	}
}

func getTime() string {
	currentTime := time.Now()
	t := currentTime.Format("2006-01-02T15:04:05Z")
	return t
}

func (g *GatherInfo) RunGather() error {
	start_time := time.Now()

	wg := &sync.WaitGroup{}

	cachePath := fmt.Sprintf("%s/cache_db", utils.TempPath)
	cache := NewCacheDB(cachePath)
	time.Sleep(1 * time.Second)
	workers := 100
	deliveries := g.NumberDelivereis
	retries := 100

	deliveryQueue := make(chan GatherDelivery)
	producerQueue := make(chan string)
	retriesQueue := make(chan GatherDelivery)

	if err := cache.HandleCacheDB(); err != nil {
		return err
	}

	if err := DumpCSVToCache(g.CsvPath, cache); err != nil {
		log.Fatalln(err)
	}
	g.cacheDB = cache

	if err := g.ElasticInterface.Connect(); err != nil {
		log.Fatal(err)
	}

	for p := 0; p < workers; p++ {
		go func() {
			for path := range producerQueue {
				wg.Add(1)
				g.worker(path, wg, deliveryQueue)
			}
		}()
	}

	for d := 0; d < deliveries; d++ {
		go func() {
			for delivery := range deliveryQueue {
				wg.Add(1)
				g.DeliveryToElastick(delivery, retriesQueue, wg)
			}

		}()
	}

	for r := 0; r < retries; r++ {
		go func() {
			for retry := range retriesQueue {
				wg.Add(1)
				HandleRetry(retry, deliveryQueue, wg)
			}
		}()
	}

	for _, path := range g.ChunkPath {
		producerQueue <- path
	}

	wg.Wait()

	close(producerQueue)
	close(deliveryQueue)
	close(retriesQueue)

	cache.DB.Close()
	cache.DropDB()

	finish_time := time.Now()
	duration := finish_time.Sub(start_time)

	log.Printf("total doc sended to elastic is: %d | duration: %v", COUNT, duration)

	return nil
}

func (g *GatherProsses) MetaData(c *CacheDB, mmdbPath string) error {
	if err := g.QueryCsvFromCache(c, g.doc.ASN); err != nil {
		log.Println(err)
	}
	mmdb := NewMMDB(g.doc.Prefix)
	if err := mmdb.HandleMMDB(mmdbPath); err != nil {
		return err
	}
	mmdb.FindCidr()
	g.doc.CountryCode = mmdb.CountryCode
	g.doc.PrefixVersion = mmdb.PrefixVersion
	return nil
}

func (g *GatherProsses) QueryCsvFromCache(c *CacheDB, asn int) error {
	asnDesc, err := c.Get(fmt.Sprintf("%d", asn))
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return fmt.Errorf("warning :: AS%d not found in db.", asn)
		}
		return err
	}

	g.doc.AsDescription = asnDesc
	return nil
}

func DumpCSVToCache(path string, c *CacheDB) error {

	csvFile, err := os.Open(path)
	if err != nil {
		return err
	}
	defer csvFile.Close()
	ObjreaderCSV := csv.NewReader(csvFile)
	records, err := ObjreaderCSV.ReadAll()
	cnt := 0
	for i, records := range records {
		if i == 0 {
			continue
		}
		replacedStr := strings.Replace(records[1], "\"", "'", -1)
		if err := c.Set(records[0][2:], replacedStr); err != nil {
			log.Fatalln(err)
		}
		cnt = i
	}
	log.Println("number of save csv to cache db: ", cnt)
	return nil
}

func (g *GatherInfo) worker(path string, wg *sync.WaitGroup, ch chan GatherDelivery) {
	defer wg.Done()
	tableFile, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer tableFile.Close()

	var elasticDocs = []*ElasticDocs{}
	scannerTable := bufio.NewScanner(tableFile)
	for scannerTable.Scan() {
		table := JsonlField{}
		line := scannerTable.Bytes()
		err := json.Unmarshal(line, &table)
		if err != nil {
			log.Println("Error parsing line:", err)
			continue
		}

		proc := GatherProsses{
			doc: ElasticDocs{
				ASN:       table.Asn,
				Prefix:    table.Cidr,
				TimeStamp: getTime(),
			},
		}
		if err := proc.MetaData(g.cacheDB, g.GeommdbPath); err != nil {
			log.Fatal(err)
		}
		elasticDocs = append(elasticDocs, &proc.doc)
		if len(elasticDocs) == BulkNumber {
			delivery := GatherDelivery{
				Docs:       elasticDocs,
				MaxRetries: 1,
			}
			ch <- delivery
			elasticDocs = elasticDocs[:0]
		}
	}
	if len(elasticDocs) != 0 {
		delivery := GatherDelivery{
			Docs:       elasticDocs,
			MaxRetries: 1,
		}
		ch <- delivery
	}

	if err := scannerTable.Err(); err != nil {
		log.Fatal(err)
	}
}

func HandleRetry(r GatherDelivery, c chan GatherDelivery, wg *sync.WaitGroup) {
	defer wg.Done()
	if r.MaxRetries < 50 {
		time.Sleep(time.Duration(r.MaxRetries * int(time.Second)))
		r.MaxRetries++

	}
	c <- r

	prefixes := func() string {
		var prefix = []string{}
		for _, ip := range r.Docs {
			prefix = append(prefix, ip.Prefix)
		}
		JPrefix, _ := json.Marshal(prefix)
		return string(JPrefix)
	}()

	log.Printf("retry %dth , prefixes: %s", r.MaxRetries, prefixes)
}

func (g *GatherInfo) DeliveryToElastick(d GatherDelivery, r chan GatherDelivery, wg *sync.WaitGroup) {
	defer wg.Done()
	time.Sleep(500 * time.Millisecond)
	if failed, err := g.ElasticInterface.UploadBlunktoElastic(d.Docs); err != nil {
		log.Println(err)
		retry := GatherDelivery{
			Docs:       failed,
			MaxRetries: d.MaxRetries + 1,
		}
		r <- retry
	} else {
		return
	}
}
