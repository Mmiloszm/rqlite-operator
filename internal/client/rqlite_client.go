package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	rqlitev1alpha1 "github.com/mmiloszm/rqlite-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type RqliteStatusResponse struct {
	Store StoreStatus `json:"store"`
	Node  NodeInfo    `json:"node"`
}

type StoreStatus struct {
	RaftState string         `json:"raft_state"`
	Leader    RaftLeaderInfo `json:"leader"`
	Nodes     []RaftNode     `json:"nodes,omitempty"`
	Raft      RaftInfo       `json:"raft,omitempty"`
	SQLite3   SQLite3Info    `json:"sqlite3,omitempty"`
}

type RaftLeaderInfo struct {
	NodeID  string `json:"node_id"`
	Address string `json:"addr"`
}

type RaftNode struct {
	ID      string `json:"id"`
	Address string `json:"addr"`
	State   string `json:"state"`
}

type NodeInfo struct {
	StartTime string `json:"start_time"`
	Uptime    string `json:"uptime"`
}

type RaftInfo struct {
	LastContact interface{} `json:"last_contact,omitempty"`
}

type SQLite3Info struct {
	DBSize        int64         `json:"db_size,omitempty"`
	WALSize       int64         `json:"wal_size,omitempty"`
	ConnPoolStats ConnPoolStats `json:"conn_pool_stats,omitempty"`
}

type ConnPoolStats struct {
	ReadOnly  ConnectionStats `json:"ro,omitempty"`
	ReadWrite ConnectionStats `json:"rw,omitempty"`
}

type ConnectionStats struct {
	Open int64 `json:"open_connections"`
	Idle int64 `json:"idle"`
	Wait int64 `json:"wait_count"`
}

type RqliteClient struct {
	httpClient *http.Client
	namespace  string
}

func NewRqliteClient(namespace string) *RqliteClient {
	return &RqliteClient{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		namespace: namespace,
	}
}

func (c *RqliteClient) GetClusterStatus(ctx context.Context, clusterName string) (*RqliteStatusResponse, error) {
	url := fmt.Sprintf("http://%s-client.%s.svc:4001/status", clusterName, c.namespace)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("rqlite API returned status %d", resp.StatusCode)
	}

	var statusResp RqliteStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&statusResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &statusResp, nil
}

func ConvertToNodeStatuses(status *RqliteStatusResponse) []rqlitev1alpha1.NodeStatus {
	var nodes []rqlitev1alpha1.NodeStatus

	for _, node := range status.Store.Nodes {
		nodeStatus := rqlitev1alpha1.NodeStatus{
			ID:        node.ID,
			Address:   node.Address,
			State:     node.State,
			Reachable: true,
		}

		now := metav1.Now()
		nodeStatus.LastContact = &now

		nodes = append(nodes, nodeStatus)
	}

	if len(nodes) == 0 && status.Store.Leader.NodeID != "" {
		nodeStatus := rqlitev1alpha1.NodeStatus{
			ID:        status.Store.Leader.NodeID,
			Address:   status.Store.Leader.Address,
			State:     status.Store.RaftState,
			Reachable: true,
		}

		now := metav1.Now()
		nodeStatus.LastContact = &now

		nodes = append(nodes, nodeStatus)
	}

	return nodes
}

func GetLeaderInfo(status *RqliteStatusResponse) (leaderID string, leaderAddress string) {
	if status.Store.Leader.NodeID != "" {
		return status.Store.Leader.NodeID, status.Store.Leader.Address
	}

	if status.Store.RaftState == "Leader" {
		for _, node := range status.Store.Nodes {
			if node.State == "Leader" {
				return node.ID, node.Address
			}
		}
	}

	return "", ""
}

func (c *RqliteClient) ExecuteReadQuery(ctx context.Context, clusterName, nodeName string) (time.Duration, error) {
	queryURL := fmt.Sprintf("http://%s.%s-headless.%s.svc.cluster.local:4001/db/query", nodeName, clusterName, c.namespace)

	queryPayload := `["SELECT 1 as health_check"]`

	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, strings.NewReader(queryPayload))
	if err != nil {
		return 0, fmt.Errorf("failed to create query request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	latency := time.Since(start)

	if err != nil {
		return latency, fmt.Errorf("failed to execute query request: %w", err)
	}
	defer resp.Body.Close()

	// Return latency regardless of response status for timing accuracy
	// The Prometheus histogram will record all attempts (success and failure)
	return latency, nil
}
