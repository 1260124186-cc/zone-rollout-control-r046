package workflowprobe

import (
	"context"
	"net/http"
	"reflect"
	"strings"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

func releaseGroup(h *harness, groupID string, reason string) (domain.LeaseGroupRelease, error) {
	var result domain.LeaseGroupRelease
	var body any
	if reason != "" {
		body = domain.LeaseGroupReleaseInput{Reason: reason}
	}
	err := h.call(http.MethodPatch, "/v1/lease-groups/"+groupID+"/release", body, &result)
	return result, err
}

func runLeaseGroupRelease() error {
	h, err := newHarness()
	if err != nil {
		return err
	}
	defer h.close()

	for _, name := range []string{"resolver-group-01", "resolver-group-02", "resolver-group-03"} {
		if _, err := enrollNode(h, name, "east"); err != nil {
			return err
		}
	}
	plan, _, err := createActivePlan(h, "group.example", 3001, 3, 0.5, 100)
	if err != nil {
		return err
	}
	group, err := allocate(h, plan.ID, "group-lease-0001", "east", 3)
	if err != nil {
		return err
	}

	// The legacy single-lease release endpoint must keep working.
	var single domain.NodeLease
	if err := h.call(http.MethodPatch, "/v1/leases/"+group.LeaseIDs[0]+"/release", nil, &single); err != nil {
		return err
	}
	if single.State != domain.LeaseReleased {
		return ensure(false, "single lease release did not transition the lease")
	}
	// Replaying a single release stays idempotent and adds no audit.
	if err := h.call(http.MethodPatch, "/v1/leases/"+group.LeaseIDs[0]+"/release", nil, &single); err != nil {
		return err
	}

	// One lease reports a probe and reaches its terminal state.
	if _, err := submitProbe(h, group.LeaseIDs[1], "group-probe-0001", true, 3001, 10); err != nil {
		return err
	}

	reason := "incident rollback 2026-09-30"
	first, err := releaseGroup(h, group.ID, reason)
	if err != nil {
		return err
	}
	if first.GroupID != group.ID || first.PlanID != plan.ID || first.Reason != reason {
		return ensure(false, "group release summary fields were not preserved")
	}
	if first.ReleasedCount != 1 || len(first.Results) != 3 {
		return ensure(false, "group release must report one new release and three items")
	}
	wantOutcomes := []domain.LeaseReleaseOutcome{
		domain.LeaseReleaseAlreadyReleased,
		domain.LeaseReleaseAlreadyReported,
		domain.LeaseReleaseReleased,
	}
	for index, want := range wantOutcomes {
		if first.Results[index].Lease.ID != group.LeaseIDs[index] {
			return ensure(false, "group release results must follow group lease order")
		}
		if first.Results[index].Outcome != want {
			return ensure(false, "unexpected outcome for lease "+group.LeaseIDs[index])
		}
		if first.Results[index].Lease.NodeID == "" {
			return ensure(false, "group release item must include the lease view")
		}
	}

	// Replaying the whole group must not change nodes, leases or audit events.
	before, err := h.store.Snapshot(context.Background())
	if err != nil {
		return err
	}
	replay, err := releaseGroup(h, group.ID, reason)
	if err != nil {
		return err
	}
	if replay.ReleasedCount != 0 {
		return ensure(false, "replayed group release must report no new releases")
	}
	replayOutcomes := []domain.LeaseReleaseOutcome{
		domain.LeaseReleaseAlreadyReleased,
		domain.LeaseReleaseAlreadyReported,
		domain.LeaseReleaseAlreadyReleased,
	}
	for index, item := range replay.Results {
		if item.Outcome != replayOutcomes[index] {
			return ensure(false, "replayed group release changed item outcomes")
		}
	}
	after, err := h.store.Snapshot(context.Background())
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(after.AuditEvents, before.AuditEvents) {
		return ensure(false, "replayed group release appended audit events")
	}
	if !reflect.DeepEqual(after.Leases, before.Leases) {
		return ensure(false, "replayed group release changed leases")
	}
	if !reflect.DeepEqual(after.Nodes, before.Nodes) {
		return ensure(false, "replayed group release changed nodes")
	}
	for _, node := range after.Nodes {
		if node.State != domain.NodeReady {
			return ensure(false, "every node must be ready after the group is released")
		}
	}

	// Unknown groups are reported as not found.
	_, err = releaseGroup(h, "grp-missing", reason)
	if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
		return err
	}

	// Invalid input is rejected before touching state.
	_, err = releaseGroup(h, group.ID, strings.Repeat("x", domain.MaxReleaseReasonRunes+1))
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}

	// Expired leases are reported per item; an empty body must be accepted.
	if err := runExpiredGroupRelease(h); err != nil {
		return err
	}
	return nil
}

func runExpiredGroupRelease(h *harness) error {
	for _, name := range []string{"resolver-group-04", "resolver-group-05"} {
		if _, err := enrollNode(h, name, "east"); err != nil {
			return err
		}
	}
	plan, _, err := createActivePlan(h, "expire.example", 3002, 2, 0.5, 100)
	if err != nil {
		return err
	}
	group, err := allocate(h, plan.ID, "group-lease-0002", "east", 2)
	if err != nil {
		return err
	}
	h.clock.Advance(601 * time.Second)
	// Submitting a probe runs the expiry sweep first; the probe itself is
	// rejected because the lease already expired during the sweep.
	_, err = submitProbe(h, group.LeaseIDs[0], "group-probe-0002", true, 3002, 10)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseNotClaimed)); err != nil {
		return err
	}
	result, err := releaseGroup(h, group.ID, "")
	if err != nil {
		return err
	}
	if result.ReleasedCount != 0 || len(result.Results) != 2 {
		return ensure(false, "expired group release must release nothing and list both leases")
	}
	for _, item := range result.Results {
		if item.Outcome != domain.LeaseReleaseAlreadyExpired {
			return ensure(false, "expired lease must be reported as already_expired")
		}
	}
	before, err := h.store.Snapshot(context.Background())
	if err != nil {
		return err
	}
	if _, err := releaseGroup(h, group.ID, ""); err != nil {
		return err
	}
	after, err := h.store.Snapshot(context.Background())
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(after.AuditEvents, before.AuditEvents) {
		return ensure(false, "replayed expired group release appended audit events")
	}
	if !reflect.DeepEqual(after.Nodes, before.Nodes) {
		return ensure(false, "replayed expired group release changed nodes")
	}
	return nil
}
