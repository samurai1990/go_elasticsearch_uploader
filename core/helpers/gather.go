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
	TablePath        string
	GeommdbPath      string
	CsvPath          string
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

func NewGatherInfo(tablePath, geoMMdbPath, CsvPath string) *GatherInfo {
	return &GatherInfo{
		TablePath:   tablePath,
		GeommdbPath: geoMMdbPath,
		CsvPath:     CsvPath,
	}
}

func getTime() string {
	currentTime := time.Now()
	t := currentTime.Format("2006-01-02T15:04:05Z")
	return t
}

func (g *GatherInfo) MakeBuild() error {
	wg := &sync.WaitGroup{}
	c := cache.New(3*time.Minute, 5*time.Minute)

	workers := 2000
	outputQueue := make(chan GatherJson)
	processQueue := make(chan GatherJson)
	Done := make(chan bool)

	tableFile, err := os.Open(g.TablePath)
	if err != nil {
		log.Fatal(err)
	}
	defer tableFile.Close()

	DumpCSVToCache(g.CsvPath, c)

	var cnt int64
	go func() {
		for {
			select {
			case structOut, ok := <-outputQueue:
				if ok {
					wg.Done()
					g.ElasticInterface.ElasticJson = ElasticDocs(structOut)
					if err := g.ElasticInterface.UploadtoElastic(); err != nil {
						if errors.Is(err, core.ErrEmpty) {
							log.Println(err)
						} else {
							log.Fatalln(err)
						}
					} else {
						atomic.AddInt64(&cnt, 1)
					}
				} else {
					return
				}
			case <-Done:
				close(outputQueue)
			}
		}
	}()

	for i := 0; i < workers; i++ {
		go func() {
			for obj := range processQueue {
				MetaData(c, g.GeommdbPath, obj, outputQueue)
			}
		}()
	}

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
		processQueue <- result
	}
	if err := scannerTable.Err(); err != nil {
		log.Fatal(err)
	}

	wg.Wait()
	Done <- true
	fmt.Println("total task:", cnt)
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
