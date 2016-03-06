package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	riak "github.com/basho/riak-go-client"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

var LOOPDATA_FILE string = "loopdata_all.csv"
var LOOPDATA_BUCKET string = "loopdata"
var MAX_CONNECTIONS int = 3
var SLEEP_TIMEOUT time.Duration = 120 * time.Second
var count int = 0

func ReadData(file string, cluster *riak.Cluster) {

	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	r := csv.NewReader(bufio.NewReader(f))
	header, err := r.Read()
	if len(header) == 0 {

	}
	if err == io.EOF {
		log.Fatal("Empty file", file)
	}

	sem := make(chan bool, MAX_CONNECTIONS)
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		// attempt to acquire semaphore
		// block if MAX_CONNECTIONS goroutines are already running
		sem <- true
		if count%10000 == 0 {
			log.Println(count)
		}
		go func(c int) {
			result := Store(cluster, header, record)
			if !result {
				log.Println("ERROR", "sleeping", c + 2)
				time.Sleep(SLEEP_TIMEOUT)
				result = Store(cluster, header, record)
				if !result {
					log.Fatal("FATAL", "retry after timeout failed")
				}
			}
			// allow next goroutine to run
			<-sem
		}(count)
		count++
	}
	// clear out channel before exiting
	for i := 0; i < MAX_CONNECTIONS; i++ {
		sem <- true
	}
	log.Println("FINISHED LOADING")
}

func StartCluster(hosts []string) *riak.Cluster {
	var err error
        
	nodes := []*riak.Node{}
        var node *riak.Node
        for i := 0; i < len(hosts); i++ {
        	nodeOpts := &riak.NodeOptions{
			RemoteAddress: hosts[i],
		}

		if node, err = riak.NewNode(nodeOpts); err != nil {
			log.Fatal(err.Error())
		}
		nodes = append(nodes, node)
	}
        
	opts := &riak.ClusterOptions{
		Nodes: nodes,
	}

	cluster, err := riak.NewCluster(opts)
	if err != nil {
		log.Fatal(err.Error())
	}

	if err := cluster.Start(); err != nil {
		log.Fatal(err.Error())
	}
	return cluster
}

func StopCluster(cluster *riak.Cluster) {
	if err := cluster.Stop(); err != nil {
		log.Println(err.Error())
	}
}

func parseTime(starttime string) string {
	split := strings.Split(starttime, " ")
	if len(split) != 2 {
		return ""
	}
	date := split[0]
	split = strings.Split(split[1], "-")
	if len(split) != 2 {
		return ""
	}
	return date + "T" + split[0] + "Z"
}

func buildData(header []string, data []string) map[string]string {
	fields := make(map[string]string)
	for i := range data {
		if data[i] == "" {
			fields[header[i]] = "0"
		} else {
			fields[header[i]] = data[i]
		}
	}
	return fields
}

func Store(cluster *riak.Cluster, header []string, data []string) bool {
	// convert headers and data into map
	fields := buildData(header, data)
	if fields == nil {
		log.Println("failed to parse starttime")
		return false
	}

	// build key
	key := fields["detectorid"] + "-" + fields["starttime"]

	// parse starttime to timestamp format required by Solr
	fields["starttime"] = parseTime(fields["starttime"])

	// build json from fields
	content, jsonerr := json.Marshal(fields)
	if jsonerr != nil {
		log.Println(jsonerr)
		return false
	}

	obj := &riak.Object{
		ContentType:     "application/json",
		Charset:         "utf-8",
		ContentEncoding: "utf-8",
		Value:           content,
	}

	// build command to store object at key
	cmd, err := riak.NewStoreValueCommandBuilder().
		WithBucket(LOOPDATA_BUCKET).
		WithContent(obj).
		WithKey(key).
		Build()
	if err != nil {
		log.Fatal(err.Error())
	}

	// store object
	if err := cluster.Execute(cmd); err != nil {
		log.Println(err.Error())
		return false
	}
	return true
}

func RiakTest() {
	hosts := []string{"45.55.0.44:8087", "107.170.224.168:8087", "45.55.1.236:8087"}
	// hosts := []string{"45.55.0.44:8087"}
	cluster := StartCluster(hosts)
	defer StopCluster(cluster)
	ReadData("../data/"+LOOPDATA_FILE, cluster)
}

func main() {
	RiakTest()
}
