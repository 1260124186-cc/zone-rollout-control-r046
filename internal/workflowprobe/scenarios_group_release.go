package workflowprobe

import (
	"encoding/json"
	"net/http"
	"os"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runLeaseGroupRelease() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	for _, name := range []string{"resolver-group-01", "resolver-group-02", "resolver-group-03"} {
		if _, err := enrollNode(harness, name, "east"); err != nil {
			return err
		}
	}
	if _, err := enrollNode(harness, "resolver-group-west-1", "west"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "group-release.example", 3001, 3, 0.5, 100)
	if err != nil {
		return err
	}

	// Group A: three claimed east leases.
	groupA, err := allocate(harness, plan.ID, "group-release-alloc-a", "east", 3)
	if err != nil {
		return err
	}
	if len(groupA.LeaseIDs) != 3 {
		return ensure(false, "group A must contain three leases")
	}

	// The original single-lease endpoint must keep working and stay compatible.
	var single domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+groupA.LeaseIDs[0]+"/release", nil, &single); err != nil {
		return err
	}
	if single.State != domain.LeaseReleased {
		return ensure(false, "single release did not update state")
	}

	// Batch release the whole group with one shared reason.
	var first domain.GroupReleaseResult
	if err := harness.call(
		http.MethodPatch,
		"/v1/lease-groups/"+groupA.ID+"/release",
		domain.GroupReleaseInput{Reason: "incident rollback batch"},
		&first,
	); err != nil {
		return err
	}
	if first.Reason != "incident rollback batch" || first.PlanID != plan.ID || first.GroupID != groupA.ID {
		return ensure(false, "batch release did not echo group context and reason")
	}
	if first.ReleasedCount != 2 || first.UnchangedCount != 1 {
		return ensure(false, "batch release must release two and report one idempotent item")
	}
	if len(first.Items) != 3 {
		return ensure(false, "batch release must return one item per lease")
	}
	expectedStatus := []domain.LeaseReleaseStatus{
		domain.LeaseReleaseAlreadyReleased,
		domain.LeaseReleaseReleased,
		domain.LeaseReleaseReleased,
	}
	for index, want := range expectedStatus {
		item := first.Items[index]
		if item.LeaseID != groupA.LeaseIDs[index] {
			return ensure(false, "result items must follow group lease order")
		}
		if item.Status != want || item.State != domain.LeaseReleased {
			return ensure(false, "unexpected batch release item status")
		}
		if item.Lease.State != domain.LeaseReleased || item.Lease.FinishedAt == nil {
			return ensure(false, "released lease did not reach the shared terminal state")
		}
	}

	// No node may stay leased after the batch lands.
	var leasedPage domain.NodePage
	if err := harness.call(http.MethodGet, "/v1/nodes?state=leased", nil, &leasedPage); err != nil {
		return err
	}
	if len(leasedPage.Items) != 0 {
		return ensure(false, "batch release left nodes in the leased pool")
	}

	// Audit: the single release carries no reason, both batch releases do.
	reasons, err := releaseAuditReasons(harness.tempDir)
	if err != nil {
		return err
	}
	if len(reasons) != 3 {
		return ensure(false, "expected exactly three release audit events")
	}
	if reasons[groupA.LeaseIDs[0]] != nil {
		return ensure(false, "single release audit must not gain a reason attribute")
	}
	for _, leaseID := range groupA.LeaseIDs[1:] {
		value, ok := reasons[leaseID].(string)
		if !ok || value != "incident rollback batch" {
			return ensure(false, "batch release audit must carry the shared reason")
		}
	}

	// Repeating the same batch changes neither nodes nor audit state.
	var replay domain.GroupReleaseResult
	if err := harness.call(
		http.MethodPatch,
		"/v1/lease-groups/"+groupA.ID+"/release",
		domain.GroupReleaseInput{Reason: "incident rollback batch"},
		&replay,
	); err != nil {
		return err
	}
	if replay.ReleasedCount != 0 || replay.UnchangedCount != 3 {
		return ensure(false, "replayed batch release must not release anything")
	}
	for _, item := range replay.Items {
		if item.Status != domain.LeaseReleaseAlreadyReleased {
			return ensure(false, "replayed batch must report already_released items")
		}
	}
	reasonsAfter, err := releaseAuditReasons(harness.tempDir)
	if err != nil {
		return err
	}
	if len(reasonsAfter) != 3 {
		return ensure(false, "replayed batch release must not append audit events")
	}

	// Group B: a reported lease is explained idempotently and never released.
	groupB, err := allocate(harness, plan.ID, "group-release-alloc-b", "west", 1)
	if err != nil {
		return err
	}
	if _, err := submitProbe(harness, groupB.LeaseIDs[0], "group-release-probe-1", true, revision.Serial, 10); err != nil {
		return err
	}
	var reportedBatch domain.GroupReleaseResult
	if err := harness.call(
		http.MethodPatch,
		"/v1/lease-groups/"+groupB.ID+"/release",
		domain.GroupReleaseInput{Reason: "incident rollback reported"},
		&reportedBatch,
	); err != nil {
		return err
	}
	if reportedBatch.ReleasedCount != 0 || reportedBatch.Items[0].Status != domain.LeaseReleaseAlreadyReported {
		return ensure(false, "reported lease must be reported as already_reported")
	}
	if reportedBatch.Items[0].State != domain.LeaseReported {
		return ensure(false, "reported lease must not change terminal state")
	}

	// Group C: expired leases are explained idempotently as well.
	var groupC domain.LeaseGroup
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/leases", domain.LeaseInput{
		RequestKey:   "group-release-alloc-c",
		Region:       "east",
		LeaseSeconds: 60,
		MaxNodes:     3,
	}, &groupC); err != nil {
		return err
	}
	harness.clock.Advance(61 * time.Second)
	// Probe submission runs the expiry sweep first; the lease is expired, so
	// the submission itself is expected to fail and its error is irrelevant.
	_ = harness.call(http.MethodPatch, "/v1/leases/"+groupC.LeaseIDs[0]+"/probes", domain.ProbeInput{
		RequestKey:     "group-release-probe-x",
		Success:        true,
		ObservedSerial: revision.Serial,
		LatencyMS:      10,
		ObservedAt:     harness.clock.Now(),
	}, nil)
	var expiredBatch domain.GroupReleaseResult
	if err := harness.call(
		http.MethodPatch,
		"/v1/lease-groups/"+groupC.ID+"/release",
		domain.GroupReleaseInput{Reason: "incident rollback expired"},
		&expiredBatch,
	); err != nil {
		return err
	}
	if expiredBatch.ReleasedCount != 0 || expiredBatch.UnchangedCount != 3 {
		return ensure(false, "expired batch must not release anything")
	}
	for _, item := range expiredBatch.Items {
		if item.Status != domain.LeaseReleaseAlreadyExpired || item.State != domain.LeaseExpired {
			return ensure(false, "expired lease must be reported as already_expired")
		}
	}

	// Unknown allocation group is a plain 404.
	err = harness.call(
		http.MethodPatch,
		"/v1/lease-groups/grp-does-not-exist/release",
		domain.GroupReleaseInput{Reason: "incident rollback missing"},
		nil,
	)
	if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
		return err
	}

	// Missing or blank reason fails validation before any state work.
	err = harness.call(
		http.MethodPatch,
		"/v1/lease-groups/"+groupA.ID+"/release",
		domain.GroupReleaseInput{Reason: "   "},
		nil,
	)
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}

	// Single endpoint stays idempotent on already released leases...
	var replaySingle domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+groupA.LeaseIDs[0]+"/release", nil, &replaySingle); err != nil {
		return err
	}
	if replaySingle.State != domain.LeaseReleased {
		return ensure(false, "single release replay changed state")
	}
	// ...and still rejects a reported lease.
	err = harness.call(http.MethodPatch, "/v1/leases/"+groupB.LeaseIDs[0]+"/release", nil, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	return nil
}

// releaseAuditReasons reads the local snapshot and maps each released lease
// to the reason attribute carried by its release audit event.
func releaseAuditReasons(tempDir string) (map[string]any, error) {
	payload, err := os.ReadFile(tempDir + "/state.json")
	if err != nil {
		return nil, err
	}
	var snapshot struct {
		AuditEvents []struct {
			EntityID   string         `json:"entity_id"`
			Action     string         `json:"action"`
			Attributes map[string]any `json:"attributes"`
		} `json:"audit_events"`
	}
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return nil, err
	}
	reasons := make(map[string]any)
	for _, event := range snapshot.AuditEvents {
		if event.Action != "released" {
			continue
		}
		if event.Attributes != nil {
			reasons[event.EntityID] = event.Attributes["reason"]
		} else {
			reasons[event.EntityID] = nil
		}
	}
	return reasons, nil
}
