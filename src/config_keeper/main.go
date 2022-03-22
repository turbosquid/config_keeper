package main

import (
	"bufio"
	"bytes"
	"encoding/json"
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
	"gopkg.in/yaml.v3"
)

// VERSION of application
const VERSION = "2.2.0"

// Params holds parameters
type Params struct {
	Servers         string
	ZookeeperConn   *zk.Conn
	Paths           []string
	Destination     string
	FileType        string
	RequireAllPaths bool
	OverrideEnvVar  bool
}

func main() {
	params := parseParams()

	if len(params.Servers) > 0 {
		err := params.connectZk()
		if err != nil {
			log.Fatalf("Failed to connect to zookeeper: %s", err)
		}
		defer params.ZookeeperConn.Close()
	}

	data, err := params.read(params.Paths[0])
	if err != nil {
		log.Fatalf("Failed to read from zookeeper: %s", err)
	}
	log.Printf("Pulling from: %s", params.Paths[0])

	for i := 1; i < len(params.Paths); i++ {
		override, err := params.read(params.Paths[i])
		if err == nil {
			log.Printf("Overriding with: %s", params.Paths[i])
			switch fileType := params.FileType; fileType {
			case "env":
				data, err = combineEnv(data, override)
			case "json":
				data, err = combineJson(data, override)
			case "yaml":
				data, err = combineYaml(data, override)
			default:
				log.Fatalf("Unhandled file type: %s", fileType)
			}
			if err != nil {
				log.Printf("Error combining files: %s", err)
			}
		} else if params.RequireAllPaths {
			log.Fatalf("Failed to read from zookeeper %s: %s", params.Paths[i], err)
		} else {
			log.Printf("Ignoring path not found: %s", params.Paths[i])
		}
	}

	if params.FileType == "env" {
		data, err = filterEnv(data, params.OverrideEnvVar)
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
	flag.BoolVar(&params.RequireAllPaths, "requireall", false, "requireallpaths")
	flag.StringVar(&params.FileType, "type", "env", "type of file (env, json, yaml)")
	flag.BoolVar(&params.OverrideEnvVar, "override", false, "override system environment variables")
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

	if params.Destination == "" || len(params.Paths) < 1 {
		flag.Usage()
		os.Exit(1)
	}
	return params
}

func (params *Params) read(path string) (string, error) {
	if len(params.Servers) > 0 {
		return params.readZk(path)
	} else {
		return readFile(path)
	}
}

func (params *Params) connectZk() error {
	var err error
	params.ZookeeperConn, _, err = zk.Connect(strings.Split(params.Servers, ","), time.Second*5)
	return err
}

func (params *Params) readZk(path string) (string, error) {
	data, _, err := params.ZookeeperConn.Get(path)
	if err != nil {
		return "", err
	}
	return string(data), err
}

func readFile(path string) (string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	data, err := ioutil.ReadFile(absPath)
	return string(data), err
}

func filterEnv(input string, override bool) (string, error) {
	data := make(map[string]string)
	// Keep keys in slice because map does not preserve order
	var keys []string
	lines := stringToSlice(input)

	for _, line := range lines {
		if !isEmptyLine(line) {
			key, value, err := parseLine(line)
			if err == nil {
				// ignore comments
				if key[0:1] != "#" {
					_, present := os.LookupEnv(key)
					// ignore existing environment vars
					if override || !present {
						keys = append(keys, key)
						data[key] = value
					} else {
						log.Printf("Skipping key %s. Exists in system environment vars.", key)
					}
				}
			} else {
				log.Printf("Unable to parse line (%s): %s", err, line)
			}
		}
	}

	dataBytes := new(bytes.Buffer)
	for _, key := range keys {
		fmt.Fprintf(dataBytes, "%s=%s\n", key, data[key])
	}
	return dataBytes.String(), nil
}

func combineEnv(a string, b string) (string, error) {
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
	return dataBytes.String(), nil
}

func combineJson(a string, b string) (string, error) {
	var data, override map[string]interface{}
	if err := json.Unmarshal([]byte(a), &data); err != nil {
		return "", err
	}
	if err := json.Unmarshal([]byte(b), &override); err != nil {
		return "", err
	}
	for k, v := range override {
		data[k] = v
	}
	dataBytes, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		return "", err
	}
	return string(dataBytes), err
}

func combineYaml(a string, b string) (string, error) {
	var data, override map[string]interface{}
	if err := yaml.Unmarshal([]byte(a), &data); err != nil {
		return "", err
	}
	if err := yaml.Unmarshal([]byte(b), &override); err != nil {
		return "", err
	}
	for k, v := range override {
		data[k] = v
	}
	dataBytes, err := yaml.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(dataBytes), err
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
