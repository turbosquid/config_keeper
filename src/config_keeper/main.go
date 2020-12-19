package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
)

// VERSION of application
const VERSION = "2.0.0"

// Params holds parameters
type Params struct {
	Servers     string
	Paths       []string
	Destination string
}

// ZkConn holds zookeeper connection
type ZkConn struct {
	Servers string
	Conn    *zk.Conn
}

func main() {
	params := parseParams()

	zkc := &ZkConn{Servers: params.Servers}
	err := zkc.connectZk()
	if err != nil {
		log.Fatalf("Failed to connect to zookeeper: %s", err)
	}
	defer zkc.Conn.Close()

	data, err := zkc.readZk(params.Paths[0])
	if err != nil {
		log.Fatalf("Failed to read from zookeeper: %s", err)
	}

	for i := 1; i < len(params.Paths); i++ {
		override, err := zkc.readZk(params.Paths[i])
		if err != nil {
			log.Fatalf("Failed to read from zookeeper: %s", err)
		}
		data = combineData(data, override)
	}

	err = writeDestination(params.Destination, data)
	if err != nil {
		log.Fatalf("Failed to write to destination: %s", err)
	}
}

func writeDestination(destination string, data string) error {
	fullDestination, err := filepath.Abs(destination)
	if err != nil {
		return err
	}
	err = os.MkdirAll(filepath.Dir(fullDestination), 0744)
	if err != nil {
		return err
	}
	log.Printf("Saving to %s", fullDestination)
	return ioutil.WriteFile(fullDestination, []byte(data), 0644)
}

func parseParams() Params {
	var params Params
	binName := filepath.Base(os.Args[0])
	flag.StringVar(&params.Destination, "dest", "", "file destination")
	flag.StringVar(&params.Servers, "zk", "", "zookeeper servers comma delimited")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "\n%s %s:\n", binName, VERSION)
		fmt.Fprintf(flag.CommandLine.Output(), "\nArguments:\n")
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "  strings\n")
		fmt.Fprintf(flag.CommandLine.Output(), "    	zookeeper paths, space delimated (later paths overwrite previous)\n")
		fmt.Fprintf(flag.CommandLine.Output(), "\nExample:\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  %s -dest <file destination> -zk <zookeeper servers> <zookeeper paths>\n", binName)
	}
	flag.Parse()
	params.Paths = flag.Args()

	if params.Servers == "" || params.Destination == "" || len(params.Paths) < 1 {
		flag.Usage()
		os.Exit(1)
	}
	return params
}

func (zkc *ZkConn) connectZk() error {
	var err error
	zkc.Conn, _, err = zk.Connect(strings.Split(zkc.Servers, ","), time.Second*5)
	return err
}

func (zkc *ZkConn) readZk(path string) (string, error) {
	data, _, err := zkc.Conn.Get(path)
	if err != nil {
		return "", err
	}
	return string(data), err
}

func combineData(a string, b string) string {
	data := make(map[string]string)
	// Keep keys in slice because map does not preserve order
	var keys []string
	lines := append(stringToSlice(a), stringToSlice(b)...)

	for _, line := range lines {
		if !isEmptyLine(line) {
			key, value, err := parseLine(line)
			if err == nil {
				if !stringInSlice(key, keys) {
					keys = append(keys, key)
				}
				data[key] = value
			} else {
				log.Printf("Unable to parse line (%s): %s", err, line)
			}
		}
	}

	dataBytes := new(bytes.Buffer)
	for _, key := range keys {
		fmt.Fprintf(dataBytes, "%s=%s\n", key, data[key])
	}
	return dataBytes.String()
}

func stringToSlice(a string) []string {
	var lines []string
	scanner := bufio.NewScanner(strings.NewReader(a))
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Failed to read from zookeeper: %s", err)
	}
	return lines
}

func isEmptyLine(line string) bool {
	trimmedLine := strings.TrimSpace(line)
	return len(trimmedLine) == 0 || strings.HasPrefix(trimmedLine, "#")
}

func parseLine(line string) (key string, value string, err error) {
	keyValue := strings.SplitN(line, "=", 2)

	if len(keyValue) != 2 {
		err = errors.New("no equals exists on line")
		return
	}

	key = strings.TrimSpace(keyValue[0])
	value = keyValue[1]
	return
}

func stringInSlice(str string, list []string) bool {
	for _, v := range list {
		if v == str {
			return true
		}
	}
	return false
}
