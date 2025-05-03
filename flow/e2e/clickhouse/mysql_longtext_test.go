package clickhouse

import (
	"fmt"
	"strings"
	"testing"

	"github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/peerflow"
	"github.com/stretchr/testify/require"
)

func (s ClickHouseSuite) Test_MySQL_LongText() {
	if _, ok := s.source.(*e2e.MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_longtext"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	quotedSrcFullName := "\"" + strings.ReplaceAll(srcFullName, ".", "\".\"") + "\""
	dstTableName := "test_longtext_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			lt longtext NOT NULL
		)
	`, quotedSrcFullName)))

	largeText := strings.Repeat("This is a long text value that will test the limits of longtext handling. ", 10000) // ~700KB text
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (lt) VALUES (?)`, 
		quotedSrcFullName), largeText))

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,lt")

	largeText2 := strings.Repeat("This is another long text value for CDC testing. ", 10000) // ~700KB text
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (lt) VALUES (?)`, 
		quotedSrcFullName), largeText2))

	e2e.EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,lt")

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}
