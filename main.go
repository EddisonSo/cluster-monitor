package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"eddisonso.com/go-gfs/pkg/gfslog"
	"github.com/gorilla/websocket"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type NodeMetrics struct {
	Name            string          `json:"name"`
	CPUUsage        string          `json:"cpu_usage"`
	MemoryUsage     string          `json:"memory_usage"`
	CPUCapacity     string          `json:"cpu_capacity"`
	MemoryCapacity  string          `json:"memory_capacity"`
	CPUPercent      float64         `json:"cpu_percent"`
	MemoryPercent   float64         `json:"memory_percent"`
	DiskCapacity    int64           `json:"disk_capacity"`
	DiskUsage       int64           `json:"disk_usage"`
	DiskPercent     float64         `json:"disk_percent"`
	Conditions      []NodeCondition `json:"conditions,omitempty"`
}

type NodeCondition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

type ClusterInfo struct {
	Timestamp time.Time     `json:"timestamp"`
	Nodes     []NodeMetrics `json:"nodes"`
}

type PodMetrics struct {
	Name        string  `json:"name"`
	Namespace   string  `json:"namespace"`
	Node        string  `json:"node"`
	CPUUsage    int64   `json:"cpu_usage"`     // nanocores
	MemoryUsage int64   `json:"memory_usage"`  // bytes
	DiskUsage   int64   `json:"disk_usage"`    // bytes (ephemeral storage)
}

type PodMetricsInfo struct {
	Timestamp time.Time    `json:"timestamp"`
	Pods      []PodMetrics `json:"pods"`
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

// kubeletStats represents the response from kubelet's /stats/summary endpoint
type kubeletStats struct {
	Node struct {
		Fs struct {
			CapacityBytes  int64 `json:"capacityBytes"`
			UsedBytes      int64 `json:"usedBytes"`
			AvailableBytes int64 `json:"availableBytes"`
		} `json:"fs"`
	} `json:"node"`
	Pods []kubeletPodStats `json:"pods"`
}

type kubeletPodStats struct {
	PodRef struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"podRef"`
	EphemeralStorage *struct {
		UsedBytes int64 `json:"usedBytes"`
	} `json:"ephemeral-storage,omitempty"`
}

// metricsPodList represents the response from metrics-server for pods
type metricsPodList struct {
	Items []metricsPod `json:"items"`
}

type metricsPod struct {
	Metadata struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
	Containers []struct {
		Name  string `json:"name"`
		Usage struct {
			CPU    string `json:"cpu"`
			Memory string `json:"memory"`
		} `json:"usage"`
	} `json:"containers"`
}

// getNodeDiskStats fetches disk usage from kubelet stats/summary endpoint
func getNodeDiskStats(ctx context.Context, clientset *kubernetes.Clientset, nodeName string) (capacity, used int64, err error) {
	data, err := clientset.RESTClient().
		Get().
		AbsPath("/api/v1/nodes/" + nodeName + "/proxy/stats/summary").
		DoRaw(ctx)
	if err != nil {
		return 0, 0, err
	}

	var stats kubeletStats
	if err := json.Unmarshal(data, &stats); err != nil {
		return 0, 0, err
	}

	return stats.Node.Fs.CapacityBytes, stats.Node.Fs.UsedBytes, nil
}

const coreServicesNamespace = "default"

// getPodMetrics fetches CPU, memory, and disk usage for pods in the core services namespace
func getPodMetrics(ctx context.Context, clientset *kubernetes.Clientset) (*PodMetricsInfo, error) {
	// Get pod metrics from metrics-server for the namespace
	metricsData, err := clientset.RESTClient().
		Get().
		AbsPath("/apis/metrics.k8s.io/v1beta1/namespaces/" + coreServicesNamespace + "/pods").
		DoRaw(ctx)
	if err != nil {
		return nil, err
	}

	var metricsResponse metricsPodList
	if err := json.Unmarshal(metricsData, &metricsResponse); err != nil {
		return nil, err
	}

	// Get pod info to find which node each pod is on
	pods, err := clientset.CoreV1().Pods(coreServicesNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	// Build map of pod name -> node name
	podToNode := make(map[string]string)
	for _, pod := range pods.Items {
		podToNode[pod.Name] = pod.Spec.NodeName
	}

	// Get unique nodes that have our pods
	nodeSet := make(map[string]bool)
	for _, nodeName := range podToNode {
		if nodeName != "" {
			nodeSet[nodeName] = true
		}
	}

	// Fetch kubelet stats from each node in parallel to get disk usage per pod
	podDiskUsage := make(map[string]int64) // key: "namespace/name"
	var podDiskMu sync.Mutex
	var wg sync.WaitGroup
	for nodeName := range nodeSet {
		wg.Add(1)
		go func(nodeName string) {
			defer wg.Done()
			data, err := clientset.RESTClient().
				Get().
				AbsPath("/api/v1/nodes/" + nodeName + "/proxy/stats/summary").
				DoRaw(ctx)
			if err != nil {
				slog.Warn("Failed to get kubelet stats", "node", nodeName, "error", err)
				return
			}

			var stats kubeletStats
			if err := json.Unmarshal(data, &stats); err != nil {
				return
			}

			podDiskMu.Lock()
			for _, podStat := range stats.Pods {
				if podStat.PodRef.Namespace != coreServicesNamespace {
					continue
				}
				key := podStat.PodRef.Namespace + "/" + podStat.PodRef.Name
				if podStat.EphemeralStorage != nil {
					podDiskUsage[key] = podStat.EphemeralStorage.UsedBytes
				}
			}
			podDiskMu.Unlock()
		}(nodeName)
	}
	wg.Wait()

	// Build response
	var podMetrics []PodMetrics
	for _, item := range metricsResponse.Items {
		// Sum CPU and memory across all containers
		var totalCPU, totalMemory int64
		for _, container := range item.Containers {
			cpu := resource.MustParse(container.Usage.CPU)
			mem := resource.MustParse(container.Usage.Memory)
			totalCPU += cpu.MilliValue() * 1000000 // convert to nanocores
			totalMemory += mem.Value()
		}

		key := item.Metadata.Namespace + "/" + item.Metadata.Name
		podMetrics = append(podMetrics, PodMetrics{
			Name:        item.Metadata.Name,
			Namespace:   item.Metadata.Namespace,
			Node:        podToNode[item.Metadata.Name],
			CPUUsage:    totalCPU,
			MemoryUsage: totalMemory,
			DiskUsage:   podDiskUsage[key],
		})
	}

	return &PodMetricsInfo{
		Timestamp: time.Now(),
		Pods:      podMetrics,
	}, nil
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	logServiceAddr := flag.String("log-service", "", "Log service address (e.g., log-service:50051)")
	logSource := flag.String("log-source", "cluster-monitor", "Log source name (e.g., pod name)")
	flag.Parse()

	// Initialize logger
	if *logServiceAddr != "" {
		logger := gfslog.NewLogger(gfslog.Config{
			Source:         *logSource,
			LogServiceAddr: *logServiceAddr,
			MinLevel:       slog.LevelDebug,
		})
		slog.SetDefault(logger.Logger)
		defer logger.Close()
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		slog.Error("Failed to get in-cluster config", "error", err)
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		slog.Error("Failed to create clientset", "error", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/cluster-info", func(w http.ResponseWriter, r *http.Request) {
		handleClusterInfo(w, r, clientset)
	})

	mux.HandleFunc("/ws/cluster-info", func(w http.ResponseWriter, r *http.Request) {
		handleClusterInfoWS(w, r, clientset)
	})

	mux.HandleFunc("/pod-metrics", func(w http.ResponseWriter, r *http.Request) {
		handlePodMetrics(w, r, clientset)
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// CORS middleware
	corsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		}
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		mux.ServeHTTP(w, r)
	})

	slog.Info("Cluster monitor listening", "addr", *addr)
	if err := http.ListenAndServe(*addr, corsHandler); err != nil {
		slog.Error("HTTP server failed", "error", err)
		os.Exit(1)
	}
}

func getClusterInfo(ctx context.Context, clientset *kubernetes.Clientset) (*ClusterInfo, error) {
	// Get node metrics from metrics-server
	metricsData, err := clientset.RESTClient().
		Get().
		AbsPath("/apis/metrics.k8s.io/v1beta1/nodes").
		DoRaw(ctx)
	if err != nil {
		return nil, err
	}

	// Get node info for capacity and conditions
	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	// Parse metrics response
	var metricsResponse metricsNodeList
	if err := json.Unmarshal(metricsData, &metricsResponse); err != nil {
		return nil, err
	}

	// Build capacity and conditions map
	nodeInfo := make(map[string]*corev1.Node)
	for i := range nodes.Items {
		nodeInfo[nodes.Items[i].Name] = &nodes.Items[i]
	}

	// Fetch disk stats from all nodes in parallel
	type diskStats struct {
		capacity int64
		usage    int64
	}
	diskStatsMap := make(map[string]diskStats)
	var diskMu sync.Mutex
	var wg sync.WaitGroup
	for _, item := range metricsResponse.Items {
		wg.Add(1)
		go func(nodeName string) {
			defer wg.Done()
			if cap, used, err := getNodeDiskStats(ctx, clientset, nodeName); err == nil {
				diskMu.Lock()
				diskStatsMap[nodeName] = diskStats{capacity: cap, usage: used}
				diskMu.Unlock()
			}
		}(item.Metadata.Name)
	}
	wg.Wait()

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

		// Get disk stats from parallel fetch
		var diskCapacity, diskUsage int64
		var diskPercent float64
		if ds, ok := diskStatsMap[item.Metadata.Name]; ok {
			diskCapacity = ds.capacity
			diskUsage = ds.usage
			if diskCapacity > 0 {
				diskPercent = float64(diskUsage) / float64(diskCapacity) * 100
			}
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
			DiskCapacity:   diskCapacity,
			DiskUsage:      diskUsage,
			DiskPercent:    diskPercent,
			Conditions:     conditions,
		})
	}

	return &ClusterInfo{
		Timestamp: time.Now(),
		Nodes:     nodeMetrics,
	}, nil
}

func handleClusterInfo(w http.ResponseWriter, r *http.Request, clientset *kubernetes.Clientset) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	info, err := getClusterInfo(ctx, clientset)
	if err != nil {
		http.Error(w, "Failed to get cluster info: "+err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

func handleClusterInfoWS(w http.ResponseWriter, r *http.Request, clientset *kubernetes.Clientset) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("WebSocket upgrade failed", "error", err)
		return
	}
	defer conn.Close()

	var mu sync.Mutex
	done := make(chan struct{})

	// Read pump - handle close and pings
	go func() {
		defer close(done)
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Send initial data immediately
	sendClusterInfo(conn, &mu, clientset)

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if err := sendClusterInfo(conn, &mu, clientset); err != nil {
				slog.Error("WebSocket send failed", "error", err)
				return
			}
		}
	}
}

func sendClusterInfo(conn *websocket.Conn, mu *sync.Mutex, clientset *kubernetes.Clientset) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	info, err := getClusterInfo(ctx, clientset)
	if err != nil {
		slog.Error("Failed to get cluster info", "error", err)
		return nil // Don't close connection on transient errors
	}

	mu.Lock()
	defer mu.Unlock()
	return conn.WriteJSON(info)
}

func handlePodMetrics(w http.ResponseWriter, r *http.Request, clientset *kubernetes.Clientset) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	info, err := getPodMetrics(ctx, clientset)
	if err != nil {
		http.Error(w, "Failed to get pod metrics: "+err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}
