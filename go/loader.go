package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	//"fmt"
	riak "github.com/basho/riak-go-client"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var LOOPDATA_FILE string = "loopdata1000.csv"
var LOOPDATA_BUCKET string = "loopdata"
var MAX_CONNECTIONS int = 20
var count int = 1

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
		//log.Println("Start", count)
		go func(c int) {
			Store(cluster, header, record)
			log.Println(c)
			// allow next goroutine to run
			<-sem
		}(count)
		count++
	}
	// clear out channel before exiting
	for i := 0; i < MAX_CONNECTIONS; i++ {
		sem <- true
	}
}

func StartCluster(address string) *riak.Cluster {
	nodeOpts := &riak.NodeOptions{
		RemoteAddress: address,
	}

	var node *riak.Node
	var err error
	if node, err = riak.NewNode(nodeOpts); err != nil {
		log.Fatal(err.Error())
	}

	nodes := []*riak.Node{node}
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
		fields[header[i]] = data[i]
		if data[i] == "" {
			fields[header[i]] = "0"
		}
		if header[i] == "starttime" {
			// 2011-09-15 00:00:00-07
			starttime := parseTime(data[i])
			if starttime == "" {
				return nil
			}
			fields[header[i]] = starttime
		}
	}
	return fields
}

func Store(cluster *riak.Cluster, header []string, data []string) {
	// convert headers and data into map
	fields := buildData(header, data)
	if fields == nil {
		log.Println("failed to parse starttime")
		return
	}
	// parse timestamp to convert to epoch for key
	starttime, err := time.Parse("2006-01-02T15:04:05Z07:00", fields["starttime"])
	if err != nil {
		log.Println(err)
		return
	}

	// build json from fields
	content, jsonerr := json.Marshal(fields)
	if jsonerr != nil {
		log.Println(jsonerr)
		return
	}

	// key and oject
	key := fields["detectorid"] + "-" + strconv.FormatInt(starttime.Unix(), 10)
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

	//async := &riak.Async{Command: cmd, Wait: wg}
	//if err := cluster.ExecuteAsync(async); err != nil {
	//	log.Fatal(err.Error())
	//}

	if err := cluster.Execute(cmd); err != nil {
		log.Fatal(err.Error())
	}

	// log.Println(key, fields["starttime"])
}

func RiakTest() {
	address := "bobthemundane.ddns.net:8087"
	cluster := StartCluster(address)
	defer StopCluster(cluster)
	ReadData("../data/"+LOOPDATA_FILE, cluster)
}

func main() {
	RiakTest()
}
