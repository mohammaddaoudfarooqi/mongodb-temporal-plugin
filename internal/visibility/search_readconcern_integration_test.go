//go:build integration

package visibility_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility"
)

// TestAtlasSearchCommands_UseLocalReadConcern: Atlas
// rejects $listSearchIndexes / $search / $searchMeta under any read
// concern but "local". The plugin's client defaults to majority, so every
// Atlas Search aggregation must override it. Plain mongod rejects the
// stages themselves, so this checks the commands the store sends.
func TestAtlasSearchCommands_UseLocalReadConcern(t *testing.T) {
	_, uri := startDBWithURI(t)
	var mu sync.Mutex
	sent := map[string]bson.Raw{}
	mon := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName != "aggregate" {
			return
		}
		for _, stage := range []string{"$listSearchIndexes", "$searchMeta", "$search"} {
			if strings.Contains(e.Command.String(), `"`+stage+`"`) {
				mu.Lock()
				sent[stage] = append(bson.Raw(nil), e.Command...)
				mu.Unlock()
				return
			}
		}
	}}
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetMonitor(mon).SetReadConcern(readconcern.Majority()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	db := client.Database("temporal_vis_rc")
	ctx := context.Background()

	_ = visibility.ProbeAtlasSearchAvailable(ctx, db)
	_, _ = visibility.QueryIndexStatus(ctx, db, "visibility_executions", "visibility_executions_search")
	s := visibility.New(db, visibility.ModeAtlasSearch, "visibility_executions_search",
		visibility.WithSearchAttributeProvider(searchattribute.NewTestProvider()),
		visibility.WithSearchAttributeMapperProvider(searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{Namespace: testNS})))
	_, _ = s.ListWorkflowExecutions(ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: namespace.ID(testNS), Namespace: namespace.Name(testNS), PageSize: 5})
	_, _ = s.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespace.ID(testNS), Namespace: namespace.Name(testNS)})

	for _, stage := range []string{"$listSearchIndexes", "$search", "$searchMeta"} {
		cmd, ok := sent[stage]
		if !ok {
			t.Errorf("no aggregate with %s was sent", stage)
			continue
		}
		level, err := cmd.LookupErr("readConcern", "level")
		if err != nil || level.StringValue() != "local" {
			t.Errorf("%s sent with readConcern %v; want local", stage, cmd.Lookup("readConcern"))
		}
	}
}
