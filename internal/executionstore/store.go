package executionstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/persistence"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

const (
	executionsColl        = "executions"
	currentExecutionsColl = "current_executions"
	shardsColl            = "shards"
)

// currentDoc is the BSON projection of the current-execution pointer.
// Keyed on "<shard_id>:<namespace_id>:<workflow_id>", it records which
// run_id is the active execution for a given (ns, wf) pair. Enables
// the current-run precondition check on create.
type currentDoc struct {
	ID               string    `bson:"_id"`
	ShardID          int32     `bson:"shard_id"`
	NamespaceID      string    `bson:"namespace_id"`
	WorkflowID       string    `bson:"workflow_id"`
	RunID            string    `bson:"run_id"`
	State            int32     `bson:"state"`  // WorkflowExecutionState enum
	Status           int32     `bson:"status"` // WorkflowExecutionStatus enum
	LastWriteVersion int64     `bson:"last_write_version"`
	StartRequestID   string    `bson:"start_request_id"`
	UpdateTime       time.Time `bson:"update_time"`
}

// currentDocID keys the current-execution pointer. CHASM archetypes
// have separate ID spaces (a workflow and a CHASM execution may share a
// business ID), so non-workflow archetypes get their own key; workflows
// keep the original key so existing pointers stay valid.
func currentDocID(shardID int32, namespaceID, workflowID string, archetype chasm.ArchetypeID) string {
	if archetype == chasm.UnspecifiedArchetypeID || archetype == chasm.WorkflowArchetypeID {
		return fmt.Sprintf("%d:%s:%s", shardID, namespaceID, workflowID)
	}
	return fmt.Sprintf("%d:%s:%s:a%d", shardID, namespaceID, workflowID, archetype)
}

// CreateWorkflowExecution inserts an execution + current-pointer inside
// one multi-document transaction. The shard's RangeID is
// checked, the creation-mode precondition is enforced
// , and the transaction either commits atomically or rolls
// back. History events and tasks are not yet routed; if
// the caller supplies them the store returns InvalidArgument.
func (s *Stub) CreateWorkflowExecution(
	ctx context.Context,
	req *persistence.InternalCreateWorkflowExecutionRequest,
) (_ *persistence.InternalCreateWorkflowExecutionResponse, retErr error) {
	defer s.observeOp("CreateWorkflowExecution")(&retErr)

	if s.db == nil || req == nil {
		return nil, errors.New("executionstore: uninitialised or nil request")
	}

	doc, err := DocFromState(req.ShardID,
		req.NewWorkflowSnapshot.NamespaceID,
		req.NewWorkflowSnapshot.WorkflowID,
		req.NewWorkflowSnapshot.RunID,
		snapshotToMutableState(req.NewWorkflowSnapshot),
	)
	if err != nil {
		return nil, err
	}

	taskDocs := s.tasksToHistoryTaskDocs(req.ShardID, req.NewWorkflowSnapshot.Tasks)

	sess, err := s.db.Client().StartSession()
	if err != nil {
		return nil, internalmongo.Translate(err, "executionstore.CreateWorkflowExecution.session")
	}
	defer sess.EndSession(ctx)

	_, err = internalmongo.WithTxn[struct{}](ctx, internalmongo.NewSessionRunner(sess),
		func(sctx context.Context) (struct{}, error) {
			// Shard RangeID check.
			if err := s.assertShardRange(sctx, req.ShardID, req.RangeID); err != nil {
				return struct{}{}, err
			}
			// Current-execution precondition.
			if err := s.enforceCurrentPreconditionOnCreate(sctx, req); err != nil {
				return struct{}{}, err
			}
			// Insert execution document.
			if _, err := s.db.Collection(executionsColl).InsertOne(sctx, doc); err != nil {
				if mongo.IsDuplicateKeyError(err) {
					return struct{}{}, &persistence.WorkflowConditionFailedError{
						Msg: fmt.Sprintf("execution %s already exists", doc.ID),
					}
				}
				return struct{}{}, internalmongo.Wrap(err, "executionstore.CreateWorkflowExecution.insert")
			}
			// History events (B-1): commit atomically with the execution
			// doc so a recovery after partial failure never leaves an
			// execution pointing at half-written history.
			if err := s.appendHistoryEventsBatchIn(sctx, req.NewWorkflowNewEvents); err != nil {
				return struct{}{}, err
			}
			// Tasks carried on the snapshot (B-2): insert into
			// history_tasks in the same transaction.
			if err := s.insertTasksIn(sctx, taskDocs); err != nil {
				return struct{}{}, err
			}
			// Upsert current-execution pointer (unless BypassCurrent).
			if req.Mode != persistence.CreateWorkflowModeBypassCurrent {
				if err := s.upsertCurrentPointer(sctx, req.ShardID, req.ArchetypeID, &req.NewWorkflowSnapshot); err != nil {
					return struct{}{}, err
				}
			}
			return struct{}{}, nil
		})
	if err != nil {
		return nil, err
	}
	return &persistence.InternalCreateWorkflowExecutionResponse{}, nil
}

// assertShardRange fences the transaction on the caller's RangeID and
// returns ShardOwnershipLostError when it no longer matches. The check is
// a write, not a read: a snapshot read never conflicts with a concurrent
// UpdateShard, so a stale owner could commit after losing the shard.
// Writing the shard document makes UpdateShard wait for (or abort) the
// in-flight transaction.
//
// Trade-off: execution transactions on one shard serialise on the shard
// document; concurrent ones see WriteConflict and are retried by the
// driver's transaction loop.
func (s *Stub) assertShardRange(ctx context.Context, shardID int32, rangeID int64) error {
	res, err := s.db.Collection(shardsColl).UpdateOne(ctx,
		bson.M{"_id": fmt.Sprintf("shard:%d", shardID), "range_id": rangeID},
		bson.M{"$inc": bson.M{"txn_fence": int64(1)}})
	if err != nil {
		return internalmongo.Wrap(err, "executionstore.assertShardRange")
	}
	if res.MatchedCount == 0 {
		return &persistence.ShardOwnershipLostError{
			ShardID: shardID,
			Msg:     fmt.Sprintf("shard %d range_id mismatch (expected %d)", shardID, rangeID),
		}
	}
	return nil
}

// requireCurrentRun fails with CurrentWorkflowConditionFailedError unless
// the current-execution pointer names runID.
func (s *Stub) requireCurrentRun(ctx context.Context, shardID int32, archetype chasm.ArchetypeID, namespaceID, workflowID, runID string) error {
	curr, err := s.readCurrent(ctx, shardID, archetype, namespaceID, workflowID)
	if err != nil {
		return err
	}
	if curr == nil {
		return currentConditionFailed(nil, fmt.Sprintf("no current execution; expected run %q", runID))
	}
	if curr.RunID != runID {
		return currentConditionFailed(curr, fmt.Sprintf("current run %q; expected %q", curr.RunID, runID))
	}
	return nil
}

// requireNotCurrentRun enforces the BypassCurrent contract: the pointer
// must not name the run being written.
func (s *Stub) requireNotCurrentRun(ctx context.Context, shardID int32, archetype chasm.ArchetypeID, namespaceID, workflowID, runID string) error {
	curr, err := s.readCurrent(ctx, shardID, archetype, namespaceID, workflowID)
	if err != nil {
		return err
	}
	if curr != nil && curr.RunID == runID {
		return currentConditionFailed(curr, fmt.Sprintf("bypass-current write to the current run %q", runID))
	}
	return nil
}

// readCurrent returns the current-execution pointer, or nil when absent.
func (s *Stub) readCurrent(ctx context.Context, shardID int32, archetype chasm.ArchetypeID, namespaceID, workflowID string) (*currentDoc, error) {
	var curr currentDoc
	err := s.db.Collection(currentExecutionsColl).FindOne(ctx,
		bson.M{"_id": currentDocID(shardID, namespaceID, workflowID, archetype)}).Decode(&curr)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, internalmongo.Wrap(err, "executionstore.readCurrent")
	}
	return &curr, nil
}

// enforceCurrentPreconditionOnCreate checks whether a current-execution
// pointer exists and (depending on mode) returns the right condition
// error. The logic matches upstream ExecutionManager's interpretation
// of CreateWorkflowMode.
func (s *Stub) enforceCurrentPreconditionOnCreate(
	ctx context.Context,
	req *persistence.InternalCreateWorkflowExecutionRequest,
) error {
	currKey := currentDocID(req.ShardID, req.NewWorkflowSnapshot.NamespaceID, req.NewWorkflowSnapshot.WorkflowID, req.ArchetypeID)
	var curr currentDoc
	err := s.db.Collection(currentExecutionsColl).FindOne(ctx, bson.M{"_id": currKey}).Decode(&curr)

	switch req.Mode {
	case persistence.CreateWorkflowModeBrandNew:
		// BrandNew requires no current to exist.
		if err == nil {
			return currentConditionFailed(&curr, "BrandNew: current execution already exists")
		}
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil
		}
		return internalmongo.Wrap(err, "executionstore.enforceCurrentPreconditionOnCreate.brandnew")

	case persistence.CreateWorkflowModeUpdateCurrent:
		// UpdateCurrent requires the current to match PreviousRunID +
		// PreviousLastWriteVersion.
		if errors.Is(err, mongo.ErrNoDocuments) {
			return currentConditionFailed(nil, "UpdateCurrent: no current execution to replace")
		}
		if err != nil {
			return internalmongo.Wrap(err, "executionstore.enforceCurrentPreconditionOnCreate.update")
		}
		if curr.RunID != req.PreviousRunID {
			return currentConditionFailed(&curr, fmt.Sprintf(
				"UpdateCurrent: current run %q != PreviousRunID %q", curr.RunID, req.PreviousRunID))
		}
		if curr.LastWriteVersion != req.PreviousLastWriteVersion {
			return currentConditionFailed(&curr, fmt.Sprintf(
				"UpdateCurrent: last_write_version %d != PreviousLastWriteVersion %d",
				curr.LastWriteVersion, req.PreviousLastWriteVersion))
		}
		// Only a closed previous run may be replaced.
		if enumsspb.WorkflowExecutionState(curr.State) != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
			return currentConditionFailed(&curr, fmt.Sprintf(
				"UpdateCurrent: previous run %q is not closed (state %v)",
				curr.RunID, enumsspb.WorkflowExecutionState(curr.State)))
		}
		return nil

	case persistence.CreateWorkflowModeBypassCurrent:
		// BypassCurrent never touches the pointer; no check required.
		return nil

	default:
		return serviceerror.NewInvalidArgument(fmt.Sprintf(
			"executionstore: unsupported CreateWorkflowMode %v", req.Mode))
	}
}

// upsertCurrentPointer writes the current-execution document reflecting
// the new snapshot. shardID is passed explicitly because
// InternalWorkflowSnapshot does not carry it.
func (s *Stub) upsertCurrentPointer(
	ctx context.Context,
	shardID int32,
	archetype chasm.ArchetypeID,
	snap *persistence.InternalWorkflowSnapshot,
) error {
	doc := currentDoc{
		ID:               currentDocID(shardID, snap.NamespaceID, snap.WorkflowID, archetype),
		ShardID:          shardID,
		NamespaceID:      snap.NamespaceID,
		WorkflowID:       snap.WorkflowID,
		RunID:            snap.RunID,
		LastWriteVersion: snap.LastWriteVersion,
		UpdateTime:       time.Now().UTC(),
	}
	if snap.ExecutionState != nil {
		doc.State = int32(snap.ExecutionState.State)
		doc.Status = int32(snap.ExecutionState.Status)
		doc.StartRequestID = snap.ExecutionState.CreateRequestId
	}

	_, err := s.db.Collection(currentExecutionsColl).UpdateOne(ctx,
		bson.M{"_id": doc.ID},
		bson.M{"$set": doc},
		options.UpdateOne().SetUpsert(true))
	if err != nil {
		return internalmongo.Wrap(err, "executionstore.upsertCurrentPointer")
	}
	return nil
}

// currentConditionFailed builds a *CurrentWorkflowConditionFailedError
// populated with the actual current-run fields so the upstream manager
// layer can serve a meaningful error to the caller.
func currentConditionFailed(curr *currentDoc, msg string) error {
	e := &persistence.CurrentWorkflowConditionFailedError{Msg: msg}
	if curr != nil {
		e.RunID = curr.RunID
		e.State = enumsspb.WorkflowExecutionState(curr.State)
		e.Status = enumspb.WorkflowExecutionStatus(curr.Status)
		e.LastWriteVersion = curr.LastWriteVersion
	}
	return e
}

// snapshotToMutableState adapts an InternalWorkflowSnapshot into the
// InternalWorkflowMutableState shape DocFromState consumes. The plugin
// only reads the blob-projected fields (ExecutionInfoBlob,
// ExecutionStateBlob).
func snapshotToMutableState(snap persistence.InternalWorkflowSnapshot) *persistence.InternalWorkflowMutableState {
	return &persistence.InternalWorkflowMutableState{
		ExecutionInfo:       snap.ExecutionInfoBlob,
		ExecutionState:      snap.ExecutionStateBlob,
		NextEventID:         snap.NextEventID,
		DBRecordVersion:     snap.DBRecordVersion,
		ActivityInfos:       snap.ActivityInfos,
		TimerInfos:          snap.TimerInfos,
		ChildExecutionInfos: snap.ChildExecutionInfos,
		RequestCancelInfos:  snap.RequestCancelInfos,
		SignalInfos:         snap.SignalInfos,
		ChasmNodes:          snap.ChasmNodes,
		Checksum:            snap.Checksum,
		SignalRequestedIDs:  signalSetToSlice(snap.SignalRequestedIDs),
	}
}

func signalSetToSlice(m map[string]struct{}) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// GetCurrentExecution. Returns NotFound when no pointer
// exists.
func (s *Stub) GetCurrentExecution(
	ctx context.Context,
	req *persistence.GetCurrentExecutionRequest,
) (_ *persistence.InternalGetCurrentExecutionResponse, retErr error) {
	defer s.observeOp("GetCurrentExecution")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("executionstore: uninitialised or nil request")
	}
	var doc currentDoc
	err := s.db.Collection(currentExecutionsColl).FindOne(ctx,
		bson.M{"_id": currentDocID(req.ShardID, req.NamespaceID, req.WorkflowID, req.ArchetypeID)}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, serviceerror.NewNotFound(fmt.Sprintf(
				"current execution not found: shard=%d ns=%s wf=%s",
				req.ShardID, req.NamespaceID, req.WorkflowID))
		}
		return nil, internalmongo.Translate(err, "executionstore.GetCurrentExecution")
	}
	return &persistence.InternalGetCurrentExecutionResponse{
		RunID: doc.RunID,
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:           doc.RunID,
			State:           enumsspb.WorkflowExecutionState(doc.State),
			Status:          enumspb.WorkflowExecutionStatus(doc.Status),
			CreateRequestId: doc.StartRequestID,
		},
	}, nil
}

// ConflictResolveWorkflowExecution atomically replaces the resetting
// workflow's mutable state, optionally inserts a new workflow, and
// optionally updates the current workflow.
func (s *Stub) ConflictResolveWorkflowExecution(
	ctx context.Context,
	req *persistence.InternalConflictResolveWorkflowExecutionRequest,
) (retErr error) {
	defer s.observeOp("ConflictResolveWorkflowExecution")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("executionstore: uninitialised or nil request")
	}

	reset := req.ResetWorkflowSnapshot
	resetState := snapshotToMutableState(reset)
	resetDoc, err := DocFromState(req.ShardID, reset.NamespaceID, reset.WorkflowID, reset.RunID, resetState)
	if err != nil {
		return err
	}

	sess, err := s.db.Client().StartSession()
	if err != nil {
		return internalmongo.Translate(err, "executionstore.ConflictResolveWorkflowExecution.session")
	}
	defer sess.EndSession(ctx)

	_, err = internalmongo.WithTxn[struct{}](ctx, internalmongo.NewSessionRunner(sess),
		func(sctx context.Context) (struct{}, error) {
			if err := s.assertShardRange(sctx, req.ShardID, req.RangeID); err != nil {
				return struct{}{}, err
			}
			// Current-pointer precondition: with a current
			// mutation the pointer must name that run, otherwise the reset
			// run; BypassCurrent forbids the pointer naming the reset run.
			switch req.Mode {
			case persistence.ConflictResolveWorkflowModeUpdateCurrent:
				expected := reset.RunID
				if req.CurrentWorkflowMutation != nil {
					expected = req.CurrentWorkflowMutation.RunID
				}
				if err := s.requireCurrentRun(sctx, req.ShardID, req.ArchetypeID, reset.NamespaceID, reset.WorkflowID, expected); err != nil {
					return struct{}{}, err
				}
			case persistence.ConflictResolveWorkflowModeBypassCurrent:
				if err := s.requireNotCurrentRun(sctx, req.ShardID, req.ArchetypeID, reset.NamespaceID, reset.WorkflowID, reset.RunID); err != nil {
					return struct{}{}, err
				}
			}
			// Reset: replace the resetting execution under CAS.
			res, err := s.db.Collection(executionsColl).ReplaceOne(sctx,
				casFilter(resetDoc.ID, reset.Condition, reset.DBRecordVersion), resetDoc)
			if err != nil {
				return struct{}{}, internalmongo.Wrap(err, "executionstore.ConflictResolveWorkflowExecution.reset")
			}
			if res.MatchedCount == 0 {
				return struct{}{}, s.conditionFailed(sctx, resetDoc.ID, "ConflictResolveWorkflowExecution reset",
					reset.Condition, reset.DBRecordVersion)
			}
			// History events + tasks for the reset workflow (B-1/B-2).
			if err := s.appendHistoryEventsBatchIn(sctx, req.ResetWorkflowEventsNewEvents); err != nil {
				return struct{}{}, err
			}
			if err := s.insertTasksIn(sctx, s.tasksToHistoryTaskDocs(req.ShardID, reset.Tasks)); err != nil {
				return struct{}{}, err
			}

			// Optional new workflow.
			if req.NewWorkflowSnapshot != nil {
				nw := *req.NewWorkflowSnapshot
				nwState := snapshotToMutableState(nw)
				nwDoc, err := DocFromState(req.ShardID, nw.NamespaceID, nw.WorkflowID, nw.RunID, nwState)
				if err != nil {
					return struct{}{}, err
				}
				if _, err := s.db.Collection(executionsColl).InsertOne(sctx, nwDoc); err != nil {
					if mongo.IsDuplicateKeyError(err) {
						return struct{}{}, &persistence.WorkflowConditionFailedError{
							Msg: fmt.Sprintf("ConflictResolve: new workflow %s already exists", nwDoc.ID),
						}
					}
					return struct{}{}, internalmongo.Wrap(err, "executionstore.ConflictResolveWorkflowExecution.newwf")
				}
				if err := s.appendHistoryEventsBatchIn(sctx, req.NewWorkflowEventsNewEvents); err != nil {
					return struct{}{}, err
				}
				if err := s.insertTasksIn(sctx, s.tasksToHistoryTaskDocs(req.ShardID, nw.Tasks)); err != nil {
					return struct{}{}, err
				}
			}

			// Optional current-workflow mutation.
			if req.CurrentWorkflowMutation != nil {
				// Mutations carry deltas: apply them incrementally, exactly
				// like UpdateWorkflowExecution.
				cm := *req.CurrentWorkflowMutation
				if err := s.applyMutationIn(sctx, req.ShardID, cm); err != nil {
					return struct{}{}, err
				}
				if err := s.appendHistoryEventsBatchIn(sctx, req.CurrentWorkflowEventsNewEvents); err != nil {
					return struct{}{}, err
				}
				if err := s.insertTasksIn(sctx, s.tasksToHistoryTaskDocs(req.ShardID, cm.Tasks)); err != nil {
					return struct{}{}, err
				}
			}

			// Current pointer depends on the mode:
			//   UpdateCurrent — point to the new workflow if present,
			//                  otherwise to the reset workflow.
			//   BypassCurrent — leave pointer alone.
			if req.Mode == persistence.ConflictResolveWorkflowModeUpdateCurrent {
				target := reset
				if req.NewWorkflowSnapshot != nil {
					target = *req.NewWorkflowSnapshot
				}
				if err := s.upsertCurrentPointer(sctx, req.ShardID, req.ArchetypeID, &target); err != nil {
					return struct{}{}, err
				}
			}
			return struct{}{}, nil
		})
	return err
}

// UpdateWorkflowExecution applies a mutation inside a transaction.
// CAS on DBRecordVersion (or NextEventID when DBRecordVersion==0).
func (s *Stub) UpdateWorkflowExecution(
	ctx context.Context,
	req *persistence.InternalUpdateWorkflowExecutionRequest,
) (retErr error) {
	defer s.observeOp("UpdateWorkflowExecution")(&retErr)

	if s.db == nil || req == nil {
		return errors.New("executionstore: uninitialised or nil request")
	}
	mut := req.UpdateWorkflowMutation

	sess, err := s.db.Client().StartSession()
	if err != nil {
		return internalmongo.Translate(err, "executionstore.UpdateWorkflowExecution.session")
	}
	defer sess.EndSession(ctx)

	_, err = internalmongo.WithTxn[struct{}](ctx, internalmongo.NewSessionRunner(sess),
		func(sctx context.Context) (struct{}, error) {
			if err := s.assertShardRange(sctx, req.ShardID, req.RangeID); err != nil {
				return struct{}{}, err
			}
			// Current-pointer precondition per mode.
			switch req.Mode {
			case persistence.UpdateWorkflowModeUpdateCurrent:
				if err := s.requireCurrentRun(sctx, req.ShardID, req.ArchetypeID, mut.NamespaceID, mut.WorkflowID, mut.RunID); err != nil {
					return struct{}{}, err
				}
			case persistence.UpdateWorkflowModeBypassCurrent:
				if err := s.requireNotCurrentRun(sctx, req.ShardID, req.ArchetypeID, mut.NamespaceID, mut.WorkflowID, mut.RunID); err != nil {
					return struct{}{}, err
				}
			}
			if err := s.applyMutationIn(sctx, req.ShardID, mut); err != nil {
				return struct{}{}, err
			}
			// History events for the updated workflow + any new workflow
			// started as part of this transaction (B-1).
			if err := s.appendHistoryEventsBatchIn(sctx, req.UpdateWorkflowNewEvents); err != nil {
				return struct{}{}, err
			}
			if err := s.appendHistoryEventsBatchIn(sctx, req.NewWorkflowNewEvents); err != nil {
				return struct{}{}, err
			}
			// Tasks (B-2): mutation tasks AND the optional new-workflow
			// snapshot's tasks all commit here.
			if err := s.insertTasksIn(sctx, s.tasksToHistoryTaskDocs(req.ShardID, mut.Tasks)); err != nil {
				return struct{}{}, err
			}
			if req.NewWorkflowSnapshot != nil {
				// A new workflow started atomically with the update; write
				// its doc and tasks in the same txn.
				nw := *req.NewWorkflowSnapshot
				nwState := snapshotToMutableState(nw)
				nwDoc, err := DocFromState(req.ShardID, nw.NamespaceID, nw.WorkflowID, nw.RunID, nwState)
				if err != nil {
					return struct{}{}, err
				}
				if _, err := s.db.Collection(executionsColl).InsertOne(sctx, nwDoc); err != nil {
					if mongo.IsDuplicateKeyError(err) {
						return struct{}{}, &persistence.WorkflowConditionFailedError{
							Msg: fmt.Sprintf("UpdateWorkflowExecution: new workflow %s already exists", nwDoc.ID),
						}
					}
					return struct{}{}, internalmongo.Wrap(err, "executionstore.UpdateWorkflowExecution.newwf")
				}
				if err := s.insertTasksIn(sctx, s.tasksToHistoryTaskDocs(req.ShardID, nw.Tasks)); err != nil {
					return struct{}{}, err
				}
			}
			// UpdateCurrent: the pointer moves to the new run when one was
			// started in this transaction (continue-as-new), otherwise it
			// is refreshed for the mutated run.
			if req.Mode == persistence.UpdateWorkflowModeUpdateCurrent {
				target := mutationToSnapshot(mut)
				if req.NewWorkflowSnapshot != nil {
					target = *req.NewWorkflowSnapshot
				}
				if err := s.upsertCurrentPointer(sctx, req.ShardID, req.ArchetypeID, &target); err != nil {
					return struct{}{}, err
				}
			}
			return struct{}{}, nil
		})
	return err
}

// GetWorkflowExecution reads the full mutable state.
func (s *Stub) GetWorkflowExecution(
	ctx context.Context,
	req *persistence.GetWorkflowExecutionRequest,
) (_ *persistence.InternalGetWorkflowExecutionResponse, retErr error) {
	defer s.observeOp("GetWorkflowExecution")(&retErr)

	if s.db == nil || req == nil {
		return nil, errors.New("executionstore: uninitialised or nil request")
	}
	id := DocID(req.ShardID, req.NamespaceID, req.WorkflowID, req.RunID)
	var doc Doc
	err := s.db.Collection(executionsColl).FindOne(ctx, bson.M{"_id": id}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, serviceerror.NewNotFound(fmt.Sprintf("execution %s not found", id))
		}
		return nil, internalmongo.Translate(err, "executionstore.GetWorkflowExecution")
	}
	state, err := StateFromDoc(&doc)
	if err != nil {
		return nil, err
	}
	return &persistence.InternalGetWorkflowExecutionResponse{
		State:           state,
		DBRecordVersion: doc.DBRecordVersion,
	}, nil
}

// DeleteWorkflowExecution removes the execution document. Idempotent
// .
func (s *Stub) DeleteWorkflowExecution(
	ctx context.Context,
	req *persistence.DeleteWorkflowExecutionRequest,
) (retErr error) {
	defer s.observeOp("DeleteWorkflowExecution")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("executionstore: uninitialised or nil request")
	}
	id := DocID(req.ShardID, req.NamespaceID, req.WorkflowID, req.RunID)
	if _, err := s.db.Collection(executionsColl).DeleteOne(ctx, bson.M{"_id": id}); err != nil {
		return internalmongo.Translate(err, "executionstore.DeleteWorkflowExecution")
	}
	return nil
}

// DeleteCurrentWorkflowExecution removes the current-execution pointer
// only if its stored RunID matches the request's.
func (s *Stub) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	req *persistence.DeleteCurrentWorkflowExecutionRequest,
) (retErr error) {
	defer s.observeOp("DeleteCurrentWorkflowExecution")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("executionstore: uninitialised or nil request")
	}
	key := currentDocID(req.ShardID, req.NamespaceID, req.WorkflowID, req.ArchetypeID)
	_, err := s.db.Collection(currentExecutionsColl).DeleteOne(ctx,
		bson.M{"_id": key, "run_id": req.RunID})
	if err != nil {
		return internalmongo.Translate(err, "executionstore.DeleteCurrentWorkflowExecution")
	}
	return nil
}

// SetWorkflowExecution replaces the execution document atomically
// without CAS on DBRecordVersion.
func (s *Stub) SetWorkflowExecution(
	ctx context.Context,
	req *persistence.InternalSetWorkflowExecutionRequest,
) (retErr error) {
	defer s.observeOp("SetWorkflowExecution")(&retErr)
	if s.db == nil || req == nil {
		return errors.New("executionstore: uninitialised or nil request")
	}
	snap := req.SetWorkflowSnapshot
	state := snapshotToMutableState(snap)
	doc, err := DocFromState(req.ShardID, snap.NamespaceID, snap.WorkflowID, snap.RunID, state)
	if err != nil {
		return err
	}

	sess, err := s.db.Client().StartSession()
	if err != nil {
		return internalmongo.Translate(err, "executionstore.SetWorkflowExecution.session")
	}
	defer sess.EndSession(ctx)
	_, err = internalmongo.WithTxn[struct{}](ctx, internalmongo.NewSessionRunner(sess),
		func(sctx context.Context) (struct{}, error) {
			if err := s.assertShardRange(sctx, req.ShardID, req.RangeID); err != nil {
				return struct{}{}, err
			}
			// Replace under the snapshot CAS. A missing execution is a
			// ConditionFailedError; a version conflict is a
			// WorkflowConditionFailedError.
			res, err := s.db.Collection(executionsColl).ReplaceOne(sctx,
				casFilter(doc.ID, snap.Condition, snap.DBRecordVersion), doc)
			if err != nil {
				return struct{}{}, internalmongo.Wrap(err, "executionstore.SetWorkflowExecution.replace")
			}
			if res.MatchedCount == 0 {
				existing, _, err := s.readCASState(sctx, doc.ID)
				if err != nil {
					return struct{}{}, err
				}
				if existing == nil {
					return struct{}{}, &persistence.ConditionFailedError{
						Msg: fmt.Sprintf("SetWorkflowExecution: execution %s does not exist", doc.ID),
					}
				}
				return struct{}{}, casMiss(doc.ID, "SetWorkflowExecution", snap.Condition, snap.DBRecordVersion, existing)
			}
			return struct{}{}, nil
		})
	return err
}

// ListConcreteExecutions paginates executions within a shard, ordered by
// _id. The token is the last returned _id.
func (s *Stub) ListConcreteExecutions(
	ctx context.Context,
	req *persistence.ListConcreteExecutionsRequest,
) (_ *persistence.InternalListConcreteExecutionsResponse, retErr error) {
	defer s.observeOp("ListConcreteExecutions")(&retErr)
	if s.db == nil || req == nil {
		return nil, errors.New("executionstore: uninitialised or nil request")
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	filter := bson.M{"shard_id": req.ShardID}
	if len(req.PageToken) > 0 {
		filter["_id"] = bson.M{"$gt": string(req.PageToken)}
	}
	cursor, err := s.db.Collection(executionsColl).Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(pageSize)+1))
	if err != nil {
		return nil, internalmongo.Translate(err, "executionstore.ListConcreteExecutions")
	}
	defer cursor.Close(ctx)

	var docs []Doc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, internalmongo.Translate(err, "executionstore.ListConcreteExecutions.decode")
	}

	resp := &persistence.InternalListConcreteExecutionsResponse{}
	for i, d := range docs {
		if i == pageSize {
			resp.NextPageToken = []byte(docs[pageSize-1].ID)
			break
		}
		state, err := StateFromDoc(&d)
		if err != nil {
			return nil, err
		}
		resp.States = append(resp.States, state)
	}
	return resp, nil
}

// casFilter matches an execution document by _id and the optimistic
// concurrency predicate. dbRecordVersion is the NEW version, so the stored
// value must be dbRecordVersion-1; the legacy zero-version path compares
// next_event_id with condition instead.
func casFilter(docID string, condition, dbRecordVersion int64) bson.M {
	if dbRecordVersion > 0 {
		return bson.M{"_id": docID, "db_record_version": dbRecordVersion - 1}
	}
	return bson.M{"_id": docID, "next_event_id": condition}
}

// applyMutationIn applies a mutation's deltas to its execution document
// with the update CAS, refusing growth past the payload budget. Used by
// Update and ConflictResolve.
func (s *Stub) applyMutationIn(ctx context.Context, shardID int32, mut persistence.InternalWorkflowMutation) error {
	docID := DocID(shardID, mut.NamespaceID, mut.WorkflowID, mut.RunID)
	pipeline := mutationToUpdatePipeline(mut)

	// Update CAS: versioned records match on db_record_version only;
	// legacy (version 0) records match next_event_id against Condition
	//.
	filter := casFilter(docID, mut.Condition, mut.DBRecordVersion)
	// Size budget: the pipeline's encoded size bounds how
	// much the document can grow, so a document already within that
	// margin of the budget does not match and nothing is written.
	growth, err := pipelineSize(pipeline)
	if err != nil {
		return err
	}
	filter["$expr"] = bson.M{"$lte": bson.A{bson.M{"$bsonSize": "$$ROOT"}, int64(payloadBudget) - growth}}

	res, err := s.db.Collection(executionsColl).UpdateOne(ctx, filter, pipeline)
	if err != nil {
		return internalmongo.Wrap(err, "executionstore.applyMutation")
	}
	if res.MatchedCount > 0 {
		return nil
	}
	existing, size, err := s.readCASState(ctx, docID)
	if err != nil {
		return err
	}
	if existing != nil && casHolds(existing, mut.Condition, mut.DBRecordVersion) {
		return &persistence.TransactionSizeLimitError{
			Msg: fmt.Sprintf("execution %s would exceed the %d-byte budget (current %d bytes, mutation %d bytes)",
				docID, payloadBudget, size, growth),
		}
	}
	return casMiss(docID, "UpdateWorkflowExecution", mut.Condition, mut.DBRecordVersion, existing)
}

// conditionFailed reads the stored CAS fields and builds the
// WorkflowConditionFailedError for a snapshot write that matched nothing.
func (s *Stub) conditionFailed(ctx context.Context, docID, op string, condition, dbRecordVersion int64) error {
	existing, _, err := s.readCASState(ctx, docID)
	if err != nil {
		return err
	}
	return casMiss(docID, op, condition, dbRecordVersion, existing)
}

type casState struct {
	NextEventID     int64 `bson:"next_event_id"`
	DBRecordVersion int64 `bson:"db_record_version"`
	Size            int64 `bson:"doc_size"`
}

// readCASState returns the stored CAS fields and BSON size of an
// execution document, or nil when it does not exist.
func (s *Stub) readCASState(ctx context.Context, docID string) (*casState, int64, error) {
	var st casState
	err := s.db.Collection(executionsColl).FindOne(ctx, bson.M{"_id": docID},
		options.FindOne().SetProjection(bson.M{
			"next_event_id": 1, "db_record_version": 1, "doc_size": bson.M{"$bsonSize": "$$ROOT"},
		})).Decode(&st)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, internalmongo.Wrap(err, "executionstore.readCASState")
	}
	return &st, st.Size, nil
}

// casHolds reports whether the stored CAS fields satisfy casFilter.
func casHolds(st *casState, condition, dbRecordVersion int64) bool {
	if dbRecordVersion > 0 {
		return st.DBRecordVersion == dbRecordVersion-1
	}
	return st.NextEventID == condition
}

func casMiss(docID, op string, condition, dbRecordVersion int64, existing *casState) error {
	e := &persistence.WorkflowConditionFailedError{}
	if existing == nil {
		e.Msg = fmt.Sprintf("%s CAS miss: execution %s not found", op, docID)
		return e
	}
	e.Msg = fmt.Sprintf("%s CAS miss (condition.next_event_id=%d, new db_record_version=%d; stored next_event_id=%d, db_record_version=%d)",
		op, condition, dbRecordVersion, existing.NextEventID, existing.DBRecordVersion)
	e.NextEventID = existing.NextEventID
	e.DBRecordVersion = existing.DBRecordVersion
	return e
}

func pipelineSize(p mongo.Pipeline) (int64, error) {
	raw, err := bson.Marshal(bson.D{{Key: "p", Value: p}})
	if err != nil {
		return 0, fmt.Errorf("executionstore: encode mutation: %w", err)
	}
	return int64(len(raw)), nil
}

// mutationToSnapshot lifts a Mutation back up to a Snapshot for the
// current-pointer refresh. Only the fields the pointer writes are
// populated.
func mutationToSnapshot(mut persistence.InternalWorkflowMutation) persistence.InternalWorkflowSnapshot {
	return persistence.InternalWorkflowSnapshot{
		NamespaceID:      mut.NamespaceID,
		WorkflowID:       mut.WorkflowID,
		RunID:            mut.RunID,
		ExecutionState:   mut.ExecutionState,
		LastWriteVersion: mut.LastWriteVersion,
		StartVersion:     mut.StartVersion,
	}
}

// EnsureIndexes creates the primary compound index on executions
// ({shard_id, namespace_id, workflow_id, run_id}) and the unique index
// on current_executions _id.
func EnsureIndexes(ctx context.Context, db *mongo.Database) error {
	_, err := db.Collection(executionsColl).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "shard_id", Value: 1},
			{Key: "namespace_id", Value: 1},
			{Key: "workflow_id", Value: 1},
			{Key: "run_id", Value: 1},
		},
		Options: options.Index().SetName("executions_shard_ns_wf_run"),
	})
	if err != nil {
		return internalmongo.Translate(err, "executionstore.EnsureIndexes")
	}
	// ListConcreteExecutions scans a shard in _id order.
	_, err = db.Collection(executionsColl).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "shard_id", Value: 1}, {Key: "_id", Value: 1}},
		Options: options.Index().SetName("executions_shard_id"),
	})
	if err != nil {
		return internalmongo.Translate(err, "executionstore.EnsureIndexes")
	}
	// current_executions uses _id as the natural key — no extra index
	// needed for point lookups.
	return nil
}
