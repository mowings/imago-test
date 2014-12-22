package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
)

const SERVER = "http://localhost:3000"

type Action struct {
	Infile     string   `json:"infile"`
	Outfile    string   `json:"outfile"`
	Mimetype   string   `json:"mimetype"`
	Operations []string `json:"operations"`
}

type Request struct {
	Actions []Action `json:"actions"`
}

var total int
var errors int
var mutex = &sync.Mutex{}

func scale(worker_id int) bool {
	var err error
	defer func() {
		if err != nil {
			log.Println(err)
		}
	}()
	req := Request{
		Actions: []Action{
			{
				"s3://turbosquid-hackathon/imago/foo.png",
				fmt.Sprintf("s3://turbosquid-hackathon/imago/out/foo-%d.jpg", worker_id),
				"image/jpeg",
				[]string{"resize 200x200", "quality 100"},
			},
			{
				"s3://turbosquid-hackathon/imago/foo.png",
				fmt.Sprintf("s3://turbosquid-hackathon/imago/out/foo-%d.tiff", worker_id),
				"image/tiff",
				[]string{"resize 2000x2000"},
			},
		},
	}
	out, _ := json.Marshal(req)
	resp, err := http.Post(SERVER+"/api/v1/work", "application/json", bytes.NewBuffer(out))
	if err != nil {
		return false
	}
	if resp.StatusCode != 200 {
		log.Println("status: ", resp.Status)
		return false
	}
	var server_response map[string]interface{}
	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)
	_ = dec.Decode(&server_response)
	log.Printf("Id: %s, queue_length: %d\n", server_response["id"], int(server_response["queue_length"].(float64)))
	resp, err = http.Get(SERVER + "/api/v1/work/" + server_response["id"].(string) + "?timeout=300")
	if err != nil {
		return false
	}
	if resp.StatusCode == 200 {
		dec = json.NewDecoder(resp.Body)
		_ = dec.Decode(&server_response)
		if server_response["status"] == "error" {
			log.Println("conversion failed.")
			return false
		} else {
			log.Println("conversion complete")
		}
	} else {
		log.Printf("Conversion failed. Status: %d\n", resp.StatusCode)
		return false
	}
	return true
}

func worker(id int, count int, done chan int) {
	for i := 0; i < count; i++ {
		ok := scale(id)
		mutex.Lock()
		total++
		if !ok {
			errors++
		}
		mutex.Unlock()
	}
	done <- id
}

func main() {
	concurrency := flag.Int("concurrency", 1, "Max concurrent requests")
	count := flag.Int("count", 5, "Total requests (per worker)")
	flag.Parse()
	log.Printf("Running test with %d workers, %d requests per worker.\n", *concurrency, *count)
	c := make(chan int, *concurrency)
	for i := 0; i < *concurrency; i++ {
		go worker(i, *count, c)
	}
	for i := 0; i < *concurrency; i++ {
		id := <-c
		log.Printf("Worker %d finished\n", id)
	}
	log.Printf("Total: %d, errors: %d\n", total, errors)
}
