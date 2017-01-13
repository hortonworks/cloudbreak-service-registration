package main

import (
	"encoding/json"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
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

type ClusterResponse struct {
	Items []struct {
		Cluster struct {
			Name string `json:"cluster_name"`
		} `json:"Clusters"`
	} `json:"items"`
}

type HostsResponse struct {
	Items []struct {
		Host struct {
			HostName string `json:"host_name"`
			IP       string `json:"ip"`
		} `json:"Hosts"`
	} `json:"items"`
}

type HostComponentsResponse struct {
	Items []struct {
		Host struct {
			HostName string `json:"host_name"`
		} `json:"Hosts"`
		HostComponents []struct {
			HostRole struct {
				ComponentName string `json:"component_name"`
				Hostname      string `json:"host_name"`
				State         string `json:"state"`
			} `json:"HostRoles"`
		} `json:"host_components"`
	} `json:"items"`
}

type HostComponent struct {
	Hostname      string
	IP            string
	HostComponent string
	State         string
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

	httpClient := &http.Client{}
	clusterName := waitForCluster(httpClient, ambari)
	hosts := getHosts(httpClient, ambari)
	getHostComponents(httpClient, ambari, clusterName, hosts)
	//https://52.214.137.88/ambari/api/v1/services/?fields=components/hostComponents/RootServiceHostComponents/service_name/*
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

func createGETRequest(ambari *Ambari, path string) *http.Request {
	req, _ := http.NewRequest("GET", "http://"+ambari.Config.Address+":8080/api/v1"+path, nil)
	req.Header.Add("X-Requested-By", "ambari")
	req.SetBasicAuth(ambari.Config.Username, ambari.Config.Password)
	return req
}

func waitForCluster(client *http.Client, ambari *Ambari) string {
	req := createGETRequest(ambari, "/clusters")
	var clusterName string
	for len(clusterName) == 0 {
		resp, err := client.Do(req)
		if err != nil {
			log.Println(err)
			continue
		}
		body, _ := ioutil.ReadAll(resp.Body)
		log.Println("Clusters resonse: " + string(body))
		var cresp ClusterResponse
		decoder := json.NewDecoder(strings.NewReader(string(body)))
		if err = decoder.Decode(&cresp); err != nil {
			log.Println(err)
			continue
		}
		if len(cresp.Items[0].Cluster.Name) > 0 {
			clusterName = cresp.Items[0].Cluster.Name
		}
	}
	log.Println("Found cluster: " + clusterName)
	return clusterName
}

func getHosts(client *http.Client, ambari *Ambari) map[string]string {
	req := createGETRequest(ambari, "/hosts?fields=Hosts/ip")
	var hosts = make(map[string]string)
	for len(hosts) == 0 {
		resp, err := client.Do(req)
		if err != nil {
			log.Println(err)
			continue
		}
		body, _ := ioutil.ReadAll(resp.Body)
		log.Println("Hosts resonse: " + string(body))
		var hresp HostsResponse
		decoder := json.NewDecoder(strings.NewReader(string(body)))
		if err = decoder.Decode(&hresp); err != nil {
			log.Println(err)
			continue
		}
		if len(hresp.Items) > 0 {
			for _, item := range hresp.Items {
				hosts[item.Host.HostName] = item.Host.IP
			}
			log.Printf("Found hosts: %v", hosts)
		}
	}
	return hosts
}

func getHostComponents(client *http.Client, ambari *Ambari, clusterName string, hosts map[string]string) []HostComponent {
	req := createGETRequest(ambari, "/clusters/"+clusterName+"/hosts?fields=host_components/HostRoles/state/*")
	var hostComponents = make([]HostComponent, 0)
	for len(hostComponents) == 0 {
		resp, err := client.Do(req)
		if err != nil {
			log.Println(err)
			continue
		}
		body, _ := ioutil.ReadAll(resp.Body)
		log.Println("Host component resonse: " + string(body))
		var hresp HostComponentsResponse
		decoder := json.NewDecoder(strings.NewReader(string(body)))
		if err = decoder.Decode(&hresp); err != nil {
			log.Println(err)
		}
		log.Printf("Found host components: %v", hresp)
		if len(hresp.Items) > 0 {
			for _, item := range hresp.Items {
				ip := hosts[item.Host.HostName]
				for _, component := range item.HostComponents {
					hc := HostComponent{
						HostComponent: component.HostRole.ComponentName,
						Hostname:      item.Host.HostName,
						IP:            ip,
						State:         component.HostRole.State,
					}
					hostComponents = append(hostComponents, hc)
				}
			}
		}
	}
	log.Printf("Generated host components: %v", hostComponents)
	return hostComponents
}
