package app

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestRaftClaimAfterLeaderLoss is the dispatch contract for a 3-voter
// cluster: a task committed on the old leader must be claimable on the
// leader that replaces it, without restarting that process.
func TestRaftClaimAfterLeaderLoss(t *testing.T) {
	ports := pickThreeFreePorts(t)
	peers := map[string]string{
		topicNodeOne:   "127.0.0.1:" + ports[0],
		topicNodeTwo:   "127.0.0.1:" + ports[1],
		topicNodeThree: "127.0.0.1:" + ports[2],
	}
	nodes := make([]*raftTestNode, 3)
	for i, id := range []string{topicNodeOne, topicNodeTwo, topicNodeThree} {
		nodes[i] = startRaftNode(t, id, peers, i == 0)
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			if n != nil && !n.closed.Load() {
				_ = n.shutdown()
			}
		}
	})

	leader, probeID := waitForLeader(t, nodes, 5*time.Second)
	taskID := createTaskOnLeader(t, leader, `{"k":"before-failover"}`)
	for _, n := range nodes {
		if n == leader {
			continue
		}
		body := getTask(t, n, taskID)
		if !strings.Contains(body, taskID) {
			t.Fatalf("follower %s missing replicated task %s: %s", n.id, taskID, body)
		}
	}

	if err := leader.shutdown(); err != nil {
		t.Fatalf("leader shutdown: %v", err)
	}
	survivors := make([]*raftTestNode, 0, 2)
	for _, n := range nodes {
		if n != leader {
			survivors = append(survivors, n)
		}
	}
	newLeader, freshID := waitForLeader(t, survivors, 5*time.Second)
	if newLeader.id == leader.id {
		t.Fatalf("election returned the dead leader %s", leader.id)
	}

	got := claimTaskIDs(t, newLeader, 8)
	for _, id := range []string{probeID, taskID} {
		if _, ok := got[id]; !ok {
			t.Fatalf("new leader %s did not deliver pre-failover task %s; claimed %v (post-failover create %s)", newLeader.id, id, got, freshID)
		}
	}
	if _, ok := got[freshID]; !ok {
		t.Fatalf("new leader %s did not deliver a task it created after election: %s (claimed %v)", newLeader.id, freshID, got)
	}
}

func claimTaskIDs(t *testing.T, n *raftTestNode, max int) map[string]struct{} {
	t.Helper()
	out := make(map[string]struct{})
	for i := 0; i < max; i++ {
		req, err := http.NewRequest(http.MethodPost, n.server.URL+"/v1/codeq/tasks/claim", strings.NewReader(`{"commands":["GENERATE_MASTER"],"leaseSeconds":30}`))
		if err != nil {
			t.Fatalf("claim request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer dev-token")
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("claim on %s: %v", n.id, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusNoContent {
			return out
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("claim on %s: status %d body=%s", n.id, resp.StatusCode, body)
		}
		out[taskIDFromBody(string(body))] = struct{}{}
	}
	return out
}

func taskIDFromBody(body string) string {
	const key = `"id":"`
	i := strings.Index(body, key)
	if i < 0 {
		return ""
	}
	rest := body[i+len(key):]
	j := strings.IndexByte(rest, '"')
	if j < 0 {
		return ""
	}
	return rest[:j]
}
