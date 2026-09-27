package app

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestRaftReadinessRequiresRealQuorumWhileHTTPRemainsLive(t *testing.T) {
	ports := pickThreeFreePorts(t)
	peers := map[string]string{topicNodeOne: "127.0.0.1:" + ports[0], topicNodeTwo: "127.0.0.1:" + ports[1], topicNodeThree: "127.0.0.1:" + ports[2]}
	nodes := []*raftTestNode{startRaftNode(t, topicNodeOne, peers, true), startRaftNode(t, topicNodeTwo, peers, false), startRaftNode(t, topicNodeThree, peers, false)}
	t.Cleanup(func() {
		for _, n := range nodes {
			if !n.closed.Load() {
				_ = n.shutdown()
			} else {
				n.server.Close()
			}
		}
	})
	leader, _ := waitForLeader(t, nodes, 5*time.Second)
	response, err := http.Get(leader.server.URL + "/readyz/raft")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("healthy actual quorum status=%d", response.StatusCode)
	}
	for _, n := range nodes {
		if n != leader {
			n.closed.Store(true)
			if err := n.app.TracingShutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err = http.Get(leader.server.URL + "/readyz/raft")
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode == http.StatusServiceUnavailable {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("leader without quorum remained ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, n := range nodes {
		response, err = http.Get(n.server.URL + "/readyz/raft")
		if err != nil {
			t.Fatal("HTTP listener disappeared", err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusServiceUnavailable {
			t.Fatal("no quorum node reported ready")
		}
	}
}
