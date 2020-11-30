package main

import (
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
const VERSION = "1.0.0"

// Params is struct to hold parameters
type Params struct {
	Servers     string
	Path        string
	Destination string
}

func main() {
	params := parseParams()

	data, err := readZk(params.Servers, params.Path)
	if err != nil {
		log.Fatalf("Failed to read from zookeeper: %s", err)
	}

	err = writeDestination(params.Destination, data)
	if err != nil {
		log.Fatalf("Failed to write to destination: %s", err)
	}
}

func writeDestination(destination string, data string) error {
	fullDestination, err := filepath.Abs(destination)
	if err != nil {
		return errors.New("Unable to create absolute destination path")
	}
	log.Printf("Saving to %s", fullDestination)
	return ioutil.WriteFile(fullDestination, []byte(data), 0644)
}

func parseParams() Params {
	var params Params
	binName := filepath.Base(os.Args[0])
	flag.StringVar(&params.Servers, "zk", "", "zookeeper servers")
	flag.StringVar(&params.Path, "path", "", "zookeeper path")
	flag.StringVar(&params.Destination, "dest", "", "file destination")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "\n%s %s:\n", binName, VERSION)
		fmt.Fprintf(flag.CommandLine.Output(), "\nArguments:\n")
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "\nExample:\n")
		fmt.Fprintf(flag.CommandLine.Output(), "\t%s -zk <zookeeper servers> -path <zookeeper path> -dest <file destination>\n", binName)
	}
	flag.Parse()

	if params.Servers == "" || params.Path == "" || params.Destination == "" {
		flag.Usage()
		os.Exit(1)
	}
	return params
}

func readZk(servers string, path string) (string, error) {
	conn, _, err := zk.Connect(strings.Split(servers, ","), time.Second*5)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	data, _, err := conn.Get(path)
	if err != nil {
		return "", err
	}
	return string(data), err
}
