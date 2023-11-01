package helpers

import (
	"bgptools/core"
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"
)

type GatherJson struct {
	AsDescription string `json:"as_description"`
	ASN           int    `json:"asn"`
	CountryCode   string `json:"country_code"`
	Prefix        string `json:"prefix"`
	PrefixVersion int    `json:"Prefix_version"`
	TimeStamp     string `json:"timestamp"`
}

type GatherInfo struct {
	ChunkPath        []string
	GeommdbPath      string
	CsvPath          string
	NumberDelivereis int
	ElasticInterface *ElasticConfig
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
	wg := &sync.WaitGroup{}
	c := cache.New(3*time.Minute, 5*time.Minute)

	workers := 1000
	producers := 300
	deliveries := g.NumberDelivereis

	deliveryQueue := make(chan GatherJson)
	workerQueue := make(chan GatherJson)
	producerQueue := make(chan string)
	Done := make(chan bool)

	DumpCSVToCache(g.CsvPath, c)

	if err := g.ElasticInterface.Connect(); err != nil {
		log.Fatal(err)
	}

	var cnt int64

	for d := 0; d < deliveries; d++ {
		go func() {
			for {
				select {
				case out, ok := <-deliveryQueue:
					if ok {
						g.ElasticInterface.ElasticJson = ElasticDocs(out)
						if err := g.ElasticInterface.UploadtoElastic(); err != nil {
							if errors.Is(err, core.ErrEmpty) {
								log.Println(err)
							} else {
								log.Fatalln(err)
							}
						} else {
							atomic.AddInt64(&cnt, 1)
						}
						wg.Done()
					} else {
						return
					}
				case <-Done:
					close(deliveryQueue)
					close(workerQueue)
					close(producerQueue)
				}
			}
		}()
	}

	for w := 0; w < workers; w++ {
		go func() {
			for obj := range workerQueue {
				MetaData(c, g.GeommdbPath, obj, deliveryQueue)
			}
		}()
	}

	for p := 0; p < producers; p++ {
		go func() {
			for path := range producerQueue {
				wg.Add(1)
				Producer(path, wg, workerQueue)
			}
		}()
	}

	for _, path := range g.ChunkPath {
		producerQueue <- path
	}

	wg.Wait()
	Done <- true
	log.Printf("total doc sended to elastic is: %d", cnt)
	c.Flush()
	return nil
}

func MetaData(c *cache.Cache, mmdbPath string, s GatherJson, ch chan GatherJson) {
	asndec := QueryCsvFromCache(c, s.ASN)
	s.AsDescription = asndec
	mmdb := NewMMDB(s.Prefix)
	if err := mmdb.HandleMMDB(mmdbPath); err != nil {
		log.Fatal(err)
	}
	mmdb.FindCidr()
	s.CountryCode = mmdb.CountryCode
	s.PrefixVersion = mmdb.PrefixVersion
	ch <- s
}

func QueryCsvFromCache(c *cache.Cache, asn int) string {
	AsnDescription, found := c.Get(fmt.Sprintf("%d", asn))
	if found {
		return AsnDescription.(string)
	}
	return ""
}

func DumpCSVToCache(path string, c *cache.Cache) {
	csvFile, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer csvFile.Close()
	ObjreaderCSV := csv.NewReader(csvFile)
	records, err := ObjreaderCSV.ReadAll()
	for i, records := range records {
		if i == 0 {
			continue
		}
		replacedStr := strings.Replace(records[1], "\"", "'", -1)
		c.Set(records[0][2:], replacedStr, cache.DefaultExpiration)
	}
}

func Producer(path string, wg *sync.WaitGroup, ch chan GatherJson) {
	defer wg.Done()
	tableFile, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer tableFile.Close()

	scannerTable := bufio.NewScanner(tableFile)
	for scannerTable.Scan() {
		table := JsonlField{}
		line := scannerTable.Bytes()
		err := json.Unmarshal(line, &table)
		if err != nil {
			log.Println("Error parsing line:", err)
			continue
		}

		result := GatherJson{
			ASN:       table.Asn,
			Prefix:    table.Cidr,
			TimeStamp: getTime(),
		}
		wg.Add(1)
		ch <- result
	}
	if err := scannerTable.Err(); err != nil {
		log.Fatal(err)
	}
}
