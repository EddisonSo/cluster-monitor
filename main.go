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
	Name           string          `json:"name"`
	CPUUsage       string          `json:"cpu_usage"`
	MemoryUsage    string          `json:"memory_usage"`
	CPUCapacity    string          `json:"cpu_capacity"`
	MemoryCapacity string          `json:"memory_capacity"`
	CPUPercent     float64         `json:"cpu_percent"`
	MemoryPercent  float64         `json:"memory_percent"`
	DiskCapacity   int64           `json:"disk_capacity"`
	DiskUsage      int64           `json:"disk_usage"`
	DiskPercent    float64         `json:"disk_percent"`
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

type PodMetrics struct {
	Name           string `json:"name"`
	Namespace      string `json:"namespace"`
	Node           string `json:"node"`
	CPUUsage       int64  `json:"cpu_usage"`        // millicores
	MemoryUsage    int64  `json:"memory_usage"`     // bytes
	DiskUsage      int64  `json:"disk_usage"`       // bytes
	CPUCapacity    int64  `json:"cpu_capacity"`     // millicores (node capacity)
	MemoryCapacity int64  `json:"memory_capacity"`  // bytes (node capacity)
	DiskCapacity   int64  `json:"disk_capacity"`    // bytes (node capacity)
}

type PodMetricsInfo struct {
	Timestamp time.Time    `json:"timestamp"`
	Pods      []PodMetrics `json:"pods"`
}

// MetricsCache holds the latest metrics data updated by background workers
type MetricsCache struct {
	mu          sync.RWMutex
	clusterInfo *ClusterInfo
	podMetrics  *PodMetricsInfo
	subscribers []chan *ClusterInfo
	subMu       sync.Mutex
}

func NewMetricsCache() *MetricsCache {
	return &MetricsCache{
		clusterInfo: &ClusterInfo{Timestamp: time.Now()},
		podMetrics:  &PodMetricsInfo{Timestamp: time.Now()},
	}
}

func (c *MetricsCache) GetClusterInfo() *ClusterInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.clusterInfo
}

func (c *MetricsCache) SetClusterInfo(info *ClusterInfo) {
	c.mu.Lock()
	c.clusterInfo = info
	c.mu.Unlock()

	// Notify subscribers
	c.subMu.Lock()
	for _, ch := range c.subscribers {
		select {
		case ch <- info:
		default: // Don't block if subscriber is slow
		}
	}
	c.subMu.Unlock()
}

func (c *MetricsCache) GetPodMetrics() *PodMetricsInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.podMetrics
}

func (c *MetricsCache) SetPodMetrics(info *PodMetricsInfo) {
	c.mu.Lock()
	c.podMetrics = info
	c.mu.Unlock()
}

func (c *MetricsCache) Subscribe() chan *ClusterInfo {
	ch := make(chan *ClusterInfo, 1)
	c.subMu.Lock()
	c.subscribers = append(c.subscribers, ch)
	c.subMu.Unlock()
	return ch
}

func (c *MetricsCache) Unsubscribe(ch chan *ClusterInfo) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	for i, sub := range c.subscribers {
		if sub == ch {
			c.subscribers = append(c.subscribers[:i], c.subscribers[i+1:]...)
			close(ch)
			return
		}
	}
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

const coreServicesNamespace = "default"

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	refreshInterval := flag.Duration("refresh", 5*time.Second, "Metrics refresh interval")
	logServiceAddr := flag.String("log-service", "", "Log service address (e.g., log-service:50051)")
	logSource := flag.String("log-source", "cluster-monitor", "Log source name (e.g., pod name)")
	flag.Parse()

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

	// Initialize cache and start background workers
	cache := NewMetricsCache()
	go clusterInfoWorker(clientset, cache, *refreshInterval)
	go podMetricsWorker(clientset, cache, *refreshInterval)

	mux := http.NewServeMux()
	mux.HandleFunc("/cluster-info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(cache.GetClusterInfo())
	})

	mux.HandleFunc("/ws/cluster-info", func(w http.ResponseWriter, r *http.Request) {
		handleClusterInfoWS(w, r, cache)
	})

	mux.HandleFunc("/pod-metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(cache.GetPodMetrics())
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

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

// clusterInfoWorker fetches cluster metrics at the configured interval
func clusterInfoWorker(clientset *kubernetes.Clientset, cache *MetricsCache, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Fetch immediately on start
	fetchClusterInfo(clientset, cache)

	for range ticker.C {
		fetchClusterInfo(clientset, cache)
	}
}

func fetchClusterInfo(clientset *kubernetes.Clientset, cache *MetricsCache) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get node metrics from metrics-server
	metricsData, err := clientset.RESTClient().
		Get().
		AbsPath("/apis/metrics.k8s.io/v1beta1/nodes").
		DoRaw(ctx)
	if err != nil {
		slog.Error("Failed to get node metrics", "error", err)
		return
	}

	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		slog.Error("Failed to list nodes", "error", err)
		return
	}

	var metricsResponse metricsNodeList
	if err := json.Unmarshal(metricsData, &metricsResponse); err != nil {
		slog.Error("Failed to parse metrics", "error", err)
		return
	}

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
			data, err := clientset.RESTClient().
				Get().
				AbsPath("/api/v1/nodes/" + nodeName + "/proxy/stats/summary").
				DoRaw(ctx)
			if err != nil {
				return
			}
			var stats kubeletStats
			if err := json.Unmarshal(data, &stats); err != nil {
				return
			}
			diskMu.Lock()
			diskStatsMap[nodeName] = diskStats{
				capacity: stats.Node.Fs.CapacityBytes,
				usage:    stats.Node.Fs.UsedBytes,
			}
			diskMu.Unlock()
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

		var diskCapacity, diskUsage int64
		var diskPercent float64
		if ds, ok := diskStatsMap[item.Metadata.Name]; ok {
			diskCapacity = ds.capacity
			diskUsage = ds.usage
			if diskCapacity > 0 {
				diskPercent = float64(diskUsage) / float64(diskCapacity) * 100
			}
		}

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

	cache.SetClusterInfo(&ClusterInfo{
		Timestamp: time.Now(),
		Nodes:     nodeMetrics,
	})
}

// podMetricsWorker fetches pod metrics at the configured interval
func podMetricsWorker(clientset *kubernetes.Clientset, cache *MetricsCache, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Fetch immediately on start
	fetchPodMetrics(clientset, cache)

	for range ticker.C {
		fetchPodMetrics(clientset, cache)
	}
}

func fetchPodMetrics(clientset *kubernetes.Clientset, cache *MetricsCache) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	metricsData, err := clientset.RESTClient().
		Get().
		AbsPath("/apis/metrics.k8s.io/v1beta1/namespaces/" + coreServicesNamespace + "/pods").
		DoRaw(ctx)
	if err != nil {
		slog.Error("Failed to get pod metrics", "error", err)
		return
	}

	var metricsResponse metricsPodList
	if err := json.Unmarshal(metricsData, &metricsResponse); err != nil {
		slog.Error("Failed to parse pod metrics", "error", err)
		return
	}

	pods, err := clientset.CoreV1().Pods(coreServicesNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		slog.Error("Failed to list pods", "error", err)
		return
	}

	// Fetch nodes for capacity information
	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		slog.Error("Failed to list nodes", "error", err)
		return
	}

	type nodeCapacity struct {
		cpuMillis  int64
		memBytes   int64
		diskBytes  int64
	}
	nodeCapacities := make(map[string]nodeCapacity)
	for _, node := range nodes.Items {
		nodeCapacities[node.Name] = nodeCapacity{
			cpuMillis: node.Status.Capacity.Cpu().MilliValue(),
			memBytes:  node.Status.Capacity.Memory().Value(),
		}
	}

	podToNode := make(map[string]string)
	for _, pod := range pods.Items {
		podToNode[pod.Name] = pod.Spec.NodeName
	}

	nodeSet := make(map[string]bool)
	for _, nodeName := range podToNode {
		if nodeName != "" {
			nodeSet[nodeName] = true
		}
	}

	// Fetch kubelet stats in parallel for disk usage and disk capacity
	podDiskUsage := make(map[string]int64)
	nodeDiskCapacity := make(map[string]int64)
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
				return
			}

			var stats kubeletStats
			if err := json.Unmarshal(data, &stats); err != nil {
				return
			}

			podDiskMu.Lock()
			nodeDiskCapacity[nodeName] = stats.Node.Fs.CapacityBytes
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

	var podMetrics []PodMetrics
	for _, item := range metricsResponse.Items {
		var totalCPU, totalMemory int64
		for _, container := range item.Containers {
			cpu := resource.MustParse(container.Usage.CPU)
			mem := resource.MustParse(container.Usage.Memory)
			totalCPU += cpu.MilliValue() * 1000000
			totalMemory += mem.Value()
		}

		nodeName := podToNode[item.Metadata.Name]
		key := item.Metadata.Namespace + "/" + item.Metadata.Name

		// Get node capacity for this pod
		var cpuCap, memCap, diskCap int64
		if cap, ok := nodeCapacities[nodeName]; ok {
			cpuCap = cap.cpuMillis * 1000000 // convert to nanocores like usage
			memCap = cap.memBytes
		}
		if dc, ok := nodeDiskCapacity[nodeName]; ok {
			diskCap = dc
		}

		podMetrics = append(podMetrics, PodMetrics{
			Name:           item.Metadata.Name,
			Namespace:      item.Metadata.Namespace,
			Node:           nodeName,
			CPUUsage:       totalCPU,
			MemoryUsage:    totalMemory,
			DiskUsage:      podDiskUsage[key],
			CPUCapacity:    cpuCap,
			MemoryCapacity: memCap,
			DiskCapacity:   diskCap,
		})
	}

	cache.SetPodMetrics(&PodMetricsInfo{
		Timestamp: time.Now(),
		Pods:      podMetrics,
	})
}

func handleClusterInfoWS(w http.ResponseWriter, r *http.Request, cache *MetricsCache) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("WebSocket upgrade failed", "error", err)
		return
	}
	defer conn.Close()

	// Subscribe to updates
	updates := cache.Subscribe()
	defer cache.Unsubscribe(updates)

	done := make(chan struct{})

	// Read pump - handle close
	go func() {
		defer close(done)
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}()

	// Send current data immediately
	if err := conn.WriteJSON(cache.GetClusterInfo()); err != nil {
		return
	}

	// Send updates as they come in
	for {
		select {
		case <-done:
			return
		case info := <-updates:
			if err := conn.WriteJSON(info); err != nil {
				slog.Error("WebSocket send failed", "error", err)
				return
			}
		}
	}
}
