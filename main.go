package main

import (
	"bytes"
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
	ENV_AMBARI_CREDENTIALS_PATH         = "AMBARI_CREDENTIALS_PATH"
	ENV_AMBARI_SERVER_PATH              = "AMBARI_SERVER_PATH"
	ENV_SERVICE_CHECK_POLL_INTERVAL     = "SERVICE_CHECK_POLL_INTERVAL"
	DEFAULT_AMBARI_CREDENTIALS_PATH     = "/srv/pillar/ambari/credentials.sls"
	DEFAULT_AMBARI_SERVER_PATH          = "/srv/pillar/ambari/server.sls"
	DEFAULT_SERVICE_CHECK_POLL_INTERVAL = 10 * time.Second
	REQUEST_SLEEP_TIME                  = 5 * time.Second
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

type ConsulService struct {
	ID          string `json:"ID"`
	Name        string `json:"Name,omitempty"`
	Address     string `json:"Address"`
	Port        int64  `json:"Port"`
	ServiceName string `json:"ServiceName,omitempty"`
}

func (c *ConsulService) Json() string {
	j, _ := json.Marshal(c)
	return string(j)
}

func main() {
	ambari := createAmbariConfig()
	httpClient := &http.Client{}

	clusterName := getClusterName(httpClient, ambari)

	for {
		hosts := getHosts(httpClient, ambari)
		components := getHostComponents(httpClient, ambari, clusterName, hosts)

		consulServices := getConsulServices(httpClient)
		if newComponents := getNewComponents(components, consulServices); len(newComponents) > 0 {
			registerToConsul(httpClient, newComponents)
		}

		wait()
	}
}

func wait() {
	var sleep time.Duration
	sleepEnv := os.Getenv(ENV_SERVICE_CHECK_POLL_INTERVAL)
	if len(sleepEnv) > 0 {
		s, _ := time.ParseDuration(sleepEnv)
		sleep = s
	} else {
		sleep = DEFAULT_SERVICE_CHECK_POLL_INTERVAL
	}
	log.Printf("Sleep for %.0f seconds", sleep.Seconds())
	time.Sleep(sleep)
}

func createAmbariConfig() *Ambari {
	credentialsPath := os.Getenv(ENV_AMBARI_CREDENTIALS_PATH)
	if len(credentialsPath) == 0 {
		credentialsPath = DEFAULT_AMBARI_CREDENTIALS_PATH
	}
	log.Print("Ambari credentials path: " + credentialsPath)
	waitFile(credentialsPath)
	ambari := readCredentials(credentialsPath)

	serverPath := os.Getenv(ENV_AMBARI_SERVER_PATH)
	if len(serverPath) == 0 {
		serverPath = DEFAULT_AMBARI_SERVER_PATH
	}
	log.Print("Ambari server path: " + serverPath)
	waitFile(serverPath)
	ambari.Config.Address = readServer(serverPath).Config.Address
	return ambari
}

func waitFile(path string) {
	found := false
	for !found {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			log.Println("File not found at location: " + path)
			time.Sleep(REQUEST_SLEEP_TIME)
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
		log.Println("Read file, content: " + string(content))
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
			time.Sleep(REQUEST_SLEEP_TIME)
		}
	}
	return ambari
}

func readServer(path string) *Ambari {
	var ambari *Ambari = nil
	for ambari == nil {
		content, _ := ioutil.ReadFile(path)
		log.Println("Read file, content: " + string(content))
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
			time.Sleep(REQUEST_SLEEP_TIME)
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

func getClusterName(client *http.Client, ambari *Ambari) string {
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
		if len(cresp.Items) > 0 && len(cresp.Items[0].Cluster.Name) > 0 {
			clusterName = cresp.Items[0].Cluster.Name
		} else {
			log.Println("Cluster not found, yet, waiting..")
			time.Sleep(REQUEST_SLEEP_TIME)
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
		} else {
			log.Println("There are not hosts yet, waiting..")
			time.Sleep(REQUEST_SLEEP_TIME)
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
		} else {
			log.Println("No host components found yet, waiting..")
			time.Sleep(REQUEST_SLEEP_TIME)
		}
	}
	log.Printf("Generated host components: %v", hostComponents)
	return hostComponents
}

func getConsulServices(client *http.Client) []ConsulService {
	var registered = make([]ConsulService, 0)

	req, _ := http.NewRequest("GET", "http://localhost:8500/v1/catalog/services", nil)
	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return registered
	}
	respBody, _ := ioutil.ReadAll(resp.Body)
	log.Println("Already registered Consul services: " + string(respBody))
	var services = make(map[string]interface{})
	decoder := json.NewDecoder(strings.NewReader(string(respBody)))
	if err = decoder.Decode(&services); err != nil {
		log.Println(err)
		return registered
	}

	for service := range services {
		log.Println("Get service registrations for: " + service)
		req, _ := http.NewRequest("GET", "http://localhost:8500/v1/catalog/service/"+service, nil)
		srvResp, err := client.Do(req)
		if err != nil {
			log.Println(err)
			continue
		}
		respBody, _ := ioutil.ReadAll(srvResp.Body)
		var services []ConsulService
		decoder := json.NewDecoder(strings.NewReader(string(respBody)))
		if err = decoder.Decode(&services); err != nil {
			log.Println(err)
			continue
		}
		log.Printf("Retrieved service info: %v", services)
		for _, s := range services {
			registered = append(registered, s)
		}
	}

	return registered
}

func getNewComponents(components []HostComponent, consulServices []ConsulService) []HostComponent {
	var newComponents = make([]HostComponent, 0)
	for _, component := range components {
		registered := false
		for _, service := range consulServices {
			if service.ServiceName == strings.ToLower(component.HostComponent) && service.Address == component.IP {
				log.Printf("Service '%s' is already registered for host: %s", service.ServiceName, component.IP)
				registered = true
				break
			}
		}
		if !registered {
			newComponents = append(newComponents, component)
		}
	}
	return newComponents
}

func registerToConsul(client *http.Client, components []HostComponent) {
	for _, comp := range components {
		componentName := strings.ToLower(comp.HostComponent)
		shortHostname := comp.Hostname[0:strings.Index(comp.Hostname, ".")]
		id := strings.Replace(componentName, "_", "-", -1) + "." + strings.Replace(shortHostname, "_", "-", 1)
		service := ConsulService{
			ID:      id,
			Name:    componentName,
			Address: comp.IP,
			Port:    1080,
		}
		body := service.Json()
		log.Printf("Registering service: %v", body)
		req, _ := http.NewRequest("PUT", "http://"+comp.IP+":8500/v1/agent/service/register", bytes.NewBuffer([]byte(body)))
		req.Header.Add("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			log.Println(err)
			continue
		}
		respBody, _ := ioutil.ReadAll(resp.Body)
		if len(respBody) > 0 {
			log.Println("Invalid register request: " + string(respBody))
		}
	}
}
