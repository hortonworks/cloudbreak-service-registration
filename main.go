package main

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"time"
)

const (
	ENV_AMBARI_CREDENTIALS_PATH     = "AMBARI_CREDENTIALS_PATH"
	ENV_AMBARI_SERVER_PATH          = "AMBARI_SERVER_PATH"
	DEFAULT_AMBARI_CREDENTIALS_PATH = "/srv/pillar/ambari/credentials.sls"
	DEFAULT_AMBARI_SERVER_PATH      = "/srv/pillar/ambari/server.sls"
	SLEEP_TIME                      = 5
)

type Ambari struct {
	Config struct {
		Address  string `yaml:"server"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"ambari"`
}

func main() {
	credentialsPath := os.Getenv(ENV_AMBARI_CREDENTIALS_PATH)
	if len(credentialsPath) == 0 {
		credentialsPath = DEFAULT_AMBARI_CREDENTIALS_PATH
	}
	serverPath := os.Getenv(ENV_AMBARI_SERVER_PATH)
	if len(serverPath) == 0 {
		credentialsPath = DEFAULT_AMBARI_SERVER_PATH
	}
	waitFile(credentialsPath)
	waitFile(serverPath)
	ambari := readCredentials(credentialsPath)
	ambari.Config.Address = readServer(serverPath).Config.Address
}

func waitFile(path string) {
	found := false
	for !found {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			log.Println("File not found at location: " + path)
			time.Sleep(SLEEP_TIME * time.Second)
		} else {
			log.Println("Found file at location: " + path)
			found = true
		}
	}
}

func readCredentials(path string) *Ambari {
	var ambari *Ambari = nil
	for ambari == nil {
		content, _ := ioutil.ReadFile(path)
		var temp Ambari
		err := yaml.Unmarshal(content, &temp)
		if err != nil {
			log.Println("Cannot parse file: " + path)
			os.Exit(1)
		}
		if len(temp.Config.Username) > 0 && len(temp.Config.Password) > 0 {
			ambari = &temp
			log.Println("Ambari credentials found")
		} else {
			log.Println("Ambari credentials are empty, waiting..")
			time.Sleep(SLEEP_TIME * time.Second)
		}
	}
	return ambari
}

func readServer(path string) *Ambari {
	var ambari *Ambari = nil
	for ambari == nil {
		content, _ := ioutil.ReadFile(path)
		var temp Ambari
		err := yaml.Unmarshal(content, &temp)
		if err != nil {
			log.Println("Cannot parse file: " + path)
			os.Exit(1)
		}
		if len(temp.Config.Address) > 0 {
			ambari = &temp
			log.Println("Ambari server found")
		} else {
			log.Println("Ambari server is empty waiting..")
			time.Sleep(SLEEP_TIME * time.Second)
		}
	}
	return ambari
}
