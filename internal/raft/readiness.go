package raft

import "context"

// VerifyReadiness asks the current leader to obtain fresh peer acknowledgements.
// Local IsLeader/LeaderInfo alone are insufficient after a network partition.
func (d *DB) VerifyReadiness(ctx context.Context) error {
	future := d.raft.VerifyLeader()
	result := make(chan error, 1)
	go func() { result <- future.Error() }()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
