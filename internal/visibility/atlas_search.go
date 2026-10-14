package visibility

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
)

// ProbeAtlasSearchAvailable tries to run $listSearchIndexes on the
// visibility collection. A green return means Atlas Search is
// available; an error is returned otherwise. Use at startup when the
// plugin is configured for ModeAtlasSearch so startup fails rather
// than silently downgrading to aggregation mode.
//
// The check has to tolerate several edge cases:
//   - Collection doesn't exist yet (fresh cluster) → Atlas still
//     accepts the stage and returns an empty cursor.
//   - Running on vanilla MongoDB (no Atlas Search) → the aggregation
//     fails with the "Unrecognized pipeline stage" error.
//   - Running on Atlas but the cluster tier doesn't support Search
//     (M0/M2/M5/Flex/Serverless) → the aggregation fails with a
//     tier-related error; these tiers are unsupported.
func ProbeAtlasSearchAvailable(ctx context.Context, db *mongo.Database) error {
	if db == nil {
		return errors.New("visibility: nil database")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// $listSearchIndexes is available on any Atlas cluster with Search
	// support; on vanilla Mongo it errors out with "Unrecognized
	// pipeline stage name: '$listSearchIndexes'".
	pipeline := mongo.Pipeline{
		{{Key: "$listSearchIndexes", Value: bson.M{}}},
	}
	cur, err := searchCollection(db, executionsColl).Aggregate(ctx, pipeline)
	if err != nil {
		return classifyAtlasProbeError(err)
	}
	defer cur.Close(ctx)
	// Drain the cursor; we only care about error surface, not results.
	for cur.Next(ctx) {
	}
	if err := cur.Err(); err != nil {
		return classifyAtlasProbeError(err)
	}
	return nil
}

// classifyAtlasProbeError normalises the error message so operators
// understand what failed. Vanilla-MongoDB is the most common failure
// mode so we give it the clearest guidance.
func classifyAtlasProbeError(err error) error {
	msg := err.Error()
	low := strings.ToLower(msg)
	switch {
	case strings.Contains(low, "unrecognized pipeline stage") ||
		strings.Contains(low, "unknown pipeline stage") ||
		strings.Contains(low, "only allowed on mongodb atlas") ||
		strings.Contains(low, "location6047401"):
		return fmt.Errorf(
			"visibility: Atlas Search unavailable on this deployment "+
				"(mongod does not support $listSearchIndexes). Temporal "+
				"visibility requires MongoDB Atlas M10+ when visibilityMode=atlas-search; "+
				"switch to visibilityMode=aggregation for self-hosted deployments. "+
				"Underlying error: %s", msg)
	case strings.Contains(low, "atlas") && strings.Contains(low, "tier"):
		return fmt.Errorf(
			"visibility: Atlas cluster tier does not support Search "+
				"(M0/M2/M5/Flex/Serverless are excluded). "+
				"Underlying error: %s", msg)
	default:
		return fmt.Errorf("visibility: Atlas Search probe failed: %w", err)
	}
}

// searchCollection returns a collection handle for Atlas Search
// aggregations. $listSearchIndexes, $search and $searchMeta only run
// with read concern "local", while the plugin's client defaults to the
// configured (majority) read concern.
func searchCollection(db *mongo.Database, name string) *mongo.Collection {
	return db.Collection(name, options.Collection().SetReadConcern(readconcern.Local()))
}
