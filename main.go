package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type NodeMetrics struct {
	Name           string  `json:"name"`
	CPUUsage       string  `json:"cpu_usage"`
	MemoryUsage    string  `json:"memory_usage"`
	CPUCapacity    string  `json:"cpu_capacity"`
	MemoryCapacity string  `json:"memory_capacity"`
	CPUPercent     float64 `json:"cpu_percent"`
	MemoryPercent  float64 `json:"memory_percent"`
	Conditions     []NodeCondition `json:"conditions,omitempty"`
}

type NodeCondition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

type ClusterInfo struct {
	Timestamp time.Time     `json:"timestamp"`
	Nodes     []NodeMetrics `json:"nodes"`
}

type metricsNodeList struct {
	Items []metricsNode `json:"items"`
}

type metricsNode struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Usage struct {
		CPU    string `json:"cpu"`
		Memory string `json:"memory"`
	} `json:"usage"`
}

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Failed to get in-cluster config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create clientset: %v", err)
	}

	http.HandleFunc("/cluster-info", func(w http.ResponseWriter, r *http.Request) {
		handleClusterInfo(w, r, clientset)
	})

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	log.Printf("Cluster monitor listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}

func handleClusterInfo(w http.ResponseWriter, r *http.Request, clientset *kubernetes.Clientset) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// Get node metrics from metrics-server
	metricsData, err := clientset.RESTClient().
		Get().
		AbsPath("/apis/metrics.k8s.io/v1beta1/nodes").
		DoRaw(ctx)
	if err != nil {
		http.Error(w, "Failed to get metrics: "+err.Error(), http.StatusBadGateway)
		return
	}

	// Get node info for capacity and conditions
	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		http.Error(w, "Failed to get nodes: "+err.Error(), http.StatusBadGateway)
		return
	}

	// Parse metrics response
	var metricsResponse metricsNodeList
	if err := json.Unmarshal(metricsData, &metricsResponse); err != nil {
		http.Error(w, "Failed to parse metrics: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Build capacity and conditions map
	nodeInfo := make(map[string]*corev1.Node)
	for i := range nodes.Items {
		nodeInfo[nodes.Items[i].Name] = &nodes.Items[i]
	}

	// Build response
	var nodeMetrics []NodeMetrics
	for _, item := range metricsResponse.Items {
		node := nodeInfo[item.Metadata.Name]
		if node == nil {
			continue
		}

		cpuCapacity := node.Status.Capacity.Cpu()
		memCapacity := node.Status.Capacity.Memory()

		cpuUsage := resource.MustParse(item.Usage.CPU)
		memUsage := resource.MustParse(item.Usage.Memory)

		cpuPercent := 0.0
		if cpuCapacity.MilliValue() > 0 {
			cpuPercent = float64(cpuUsage.MilliValue()) / float64(cpuCapacity.MilliValue()) * 100
		}

		memPercent := 0.0
		if memCapacity.Value() > 0 {
			memPercent = float64(memUsage.Value()) / float64(memCapacity.Value()) * 100
		}

		// Get relevant conditions (pressure indicators)
		var conditions []NodeCondition
		for _, cond := range node.Status.Conditions {
			switch cond.Type {
			case corev1.NodeMemoryPressure, corev1.NodeDiskPressure, corev1.NodePIDPressure:
				conditions = append(conditions, NodeCondition{
					Type:   string(cond.Type),
					Status: string(cond.Status),
				})
			}
		}

		nodeMetrics = append(nodeMetrics, NodeMetrics{
			Name:           item.Metadata.Name,
			CPUUsage:       item.Usage.CPU,
			MemoryUsage:    item.Usage.Memory,
			CPUCapacity:    cpuCapacity.String(),
			MemoryCapacity: memCapacity.String(),
			CPUPercent:     cpuPercent,
			MemoryPercent:  memPercent,
			Conditions:     conditions,
		})
	}

	response := ClusterInfo{
		Timestamp: time.Now(),
		Nodes:     nodeMetrics,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
