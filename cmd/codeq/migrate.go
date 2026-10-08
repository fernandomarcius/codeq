package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func migrateCmd(ui *ui) *cobra.Command {
	return &cobra.Command{
		Use:   "migrate-shards",
		Short: "Migrate tasks between shards",
		Long: `CodeQ persists on Pebble. Queue state is not stored in Redis, so
there is no Redis shard migration.

Split a single node across Pebble shards with persistenceConfig.numShards.
That layout is local to the process and is not a Redis backend map.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintln(cmd.ErrOrStderr(), ui.info("CodeQ persists on Pebble."))
			return fmt.Errorf("redis shard migration has been removed")
		},
	}
}
