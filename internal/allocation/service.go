package allocation

import (
	"context"
	"sort"
	"strings"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/store"
)

type Service struct {
	store *store.Store
	clock platform.Clock
	ids   platform.IDGenerator
}

func New(stateStore *store.Store, clock platform.Clock, ids platform.IDGenerator) *Service {
	return &Service{store: stateStore, clock: clock, ids: ids}
}

func (s *Service) Allocate(
	ctx context.Context,
	planID string,
	input domain.LeaseInput,
) (domain.LeaseGroup, error) {
	if err := domain.ValidateLeaseInput(input); err != nil {
		return domain.LeaseGroup{}, err
	}
	input.Region = strings.TrimSpace(strings.ToLower(input.Region))
	requestIndex := planID + "\x00" + input.RequestKey
	fingerprint := domain.LeaseFingerprint(planID, input)
	now := s.clock.Now().UTC()
	var group domain.LeaseGroup

	err := s.store.Write(ctx, func(state *domain.State) error {
		plan, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		if plan.State != domain.PlanActive {
			return domain.InvalidState("plan must be active for node leasing", map[string]any{
				"plan_id": planID,
				"state":   plan.State,
			})
		}
		if existingID, exists := state.LeaseRequestKeys[requestIndex]; exists {
			existing, found := state.LeaseGroups[existingID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "lease request index is inconsistent", map[string]any{
					"lease_group_id": existingID,
				})
			}
			if existing.RequestFingerprint != fingerprint {
				return domain.NewError(domain.CodeIdempotencyConflict, "request key was reused with different lease parameters", map[string]any{
					"request_key": input.RequestKey,
				})
			}
			group = existing
			return nil
		}

		expireLeases(state, now)
		candidates := make([]domain.ResolverNode, 0)
		for _, node := range state.Nodes {
			if node.State != domain.NodeReady {
				continue
			}
			if input.Region != "" && node.Region != input.Region {
				continue
			}
			candidates = append(candidates, node)
		}
		if len(candidates) == 0 {
			return domain.NewError(domain.CodeNodeUnavailable, "no eligible node is ready", map[string]any{
				"plan_id": planID,
				"region":  input.Region,
			})
		}
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].Region != candidates[j].Region {
				return candidates[i].Region < candidates[j].Region
			}
			return candidates[i].ID < candidates[j].ID
		})
		if len(candidates) > input.MaxNodes {
			candidates = candidates[:input.MaxNodes]
		}

		groupID := s.ids.New("grp")
		leaseIDs := make([]string, 0, len(candidates))
		expiresAt := now.Add(time.Duration(input.LeaseSeconds) * time.Second)
		for _, candidate := range candidates {
			leaseID := s.ids.New("lse")
			candidate.State = domain.NodeLeased
			candidate.Version++
			candidate.UpdatedAt = now
			state.Nodes[candidate.ID] = candidate
			state.Leases[leaseID] = domain.NodeLease{
				ID:           leaseID,
				PlanID:       planID,
				NodeID:       candidate.ID,
				AllocationID: groupID,
				State:        domain.LeaseClaimed,
				ClaimedAt:    now,
				ExpiresAt:    expiresAt,
			}
			leaseIDs = append(leaseIDs, leaseID)
		}
		group = domain.LeaseGroup{
			ID:                 groupID,
			PlanID:             planID,
			RequestKey:         input.RequestKey,
			RequestFingerprint: fingerprint,
			Region:             input.Region,
			LeaseIDs:           leaseIDs,
			CreatedAt:          now,
		}
		state.LeaseGroups[group.ID] = group
		state.LeaseRequestKeys[requestIndex] = group.ID
		store.AddAudit(state, s.ids, "lease_group", group.ID, "created", now, map[string]any{
			"plan_id":     planID,
			"lease_count": len(leaseIDs),
		})
		return nil
	})
	return group, err
}

func (s *Service) Release(ctx context.Context, leaseID string) (domain.NodeLease, error) {
	now := s.clock.Now().UTC()
	var result domain.NodeLease
	err := s.store.Write(ctx, func(state *domain.State) error {
		lease, ok := state.Leases[leaseID]
		if !ok {
			return domain.NotFound("lease", leaseID)
		}
		if lease.State == domain.LeaseReleased || lease.State == domain.LeaseExpired {
			result = lease
			return nil
		}
		if lease.State != domain.LeaseClaimed {
			return domain.InvalidState("only a claimed lease can be released", map[string]any{
				"lease_id": leaseID,
				"state":    lease.State,
			})
		}
		if _, ok := state.Nodes[lease.NodeID]; !ok {
			return domain.NewError(domain.CodeStorageFailure, "lease references a missing node", map[string]any{
				"node_id": lease.NodeID,
			})
		}
		result = s.applyClaimedRelease(state, lease, "", now)
		return nil
	})
	return result, err
}

// ReleaseGroup releases every lease in an allocation group as one batch.
// All claimed leases share the same reason and the same claimed -> released
// terminal rule. Leases already in a terminal state are reported under the
// idempotency statuses and are never mutated or re-audited. Every lease and
// node reference is resolved before any terminal rule is applied, so a bad
// reference aborts the whole batch inside the single transaction instead of
// landing a partial release.
func (s *Service) ReleaseGroup(
	ctx context.Context,
	groupID string,
	input domain.GroupReleaseInput,
) (domain.GroupReleaseResult, error) {
	if err := domain.ValidateGroupReleaseInput(input); err != nil {
		return domain.GroupReleaseResult{}, err
	}
	reason := strings.TrimSpace(input.Reason)
	now := s.clock.Now().UTC()
	var result domain.GroupReleaseResult

	err := s.store.Write(ctx, func(state *domain.State) error {
		group, ok := state.LeaseGroups[groupID]
		if !ok {
			return domain.NotFound("lease_group", groupID)
		}

		// Preflight every reference first; any broken reference fails the
		// transaction before a single lease changes state.
		resolved := make([]domain.NodeLease, 0, len(group.LeaseIDs))
		for _, leaseID := range group.LeaseIDs {
			lease, found := state.Leases[leaseID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "lease group references a missing lease", map[string]any{
					"lease_group_id": groupID,
					"lease_id":       leaseID,
				})
			}
			if lease.AllocationID != groupID {
				return domain.NewError(domain.CodeStorageFailure, "lease group references a lease owned by another allocation", map[string]any{
					"lease_group_id": groupID,
					"lease_id":       leaseID,
					"allocation_id":  lease.AllocationID,
				})
			}
			if lease.State == domain.LeaseClaimed {
				if _, found := state.Nodes[lease.NodeID]; !found {
					return domain.NewError(domain.CodeStorageFailure, "lease references a missing node", map[string]any{
						"lease_id": leaseID,
						"node_id":  lease.NodeID,
					})
				}
			}
			resolved = append(resolved, lease)
		}

		items := make([]domain.LeaseReleaseItem, 0, len(resolved))
		releasedCount := 0
		for _, lease := range resolved {
			item := domain.LeaseReleaseItem{
				LeaseID: lease.ID,
				NodeID:  lease.NodeID,
			}
			if status, terminal := terminalReleaseStatus(lease.State); terminal {
				item.Status = status
			} else {
				lease = s.applyClaimedRelease(state, lease, reason, now)
				item.Status = domain.LeaseReleaseReleased
				releasedCount++
			}
			item.State = lease.State
			item.Lease = lease
			items = append(items, item)
		}

		result = domain.GroupReleaseResult{
			GroupID:        group.ID,
			PlanID:         group.PlanID,
			Reason:         reason,
			ReleasedCount:  releasedCount,
			UnchangedCount: len(items) - releasedCount,
			ReleasedAt:     now,
			Items:          items,
		}
		return nil
	})
	return result, err
}

// terminalReleaseStatus reports the idempotent status for a lease that has
// already left the claimed state and therefore cannot be released again.
func terminalReleaseStatus(state domain.LeaseState) (domain.LeaseReleaseStatus, bool) {
	switch state {
	case domain.LeaseReleased:
		return domain.LeaseReleaseAlreadyReleased, true
	case domain.LeaseExpired:
		return domain.LeaseReleaseAlreadyExpired, true
	case domain.LeaseReported:
		return domain.LeaseReleaseAlreadyReported, true
	default:
		return "", false
	}
}

// applyClaimedRelease applies the single shared claimed -> released terminal
// rule used by both the per-lease and the group-wide release flows. The
// caller must already have verified that the lease is claimed and that its
// node exists. A lease is audited once, here, only when it actually moves;
// replay of an already-terminal lease never reaches this function.
func (s *Service) applyClaimedRelease(
	state *domain.State,
	lease domain.NodeLease,
	reason string,
	now time.Time,
) domain.NodeLease {
	finishedAt := now
	lease.State = domain.LeaseReleased
	lease.FinishedAt = &finishedAt
	state.Leases[lease.ID] = lease
	if node, ok := state.Nodes[lease.NodeID]; ok && node.State == domain.NodeLeased {
		node.State = domain.NodeReady
		node.Version++
		node.UpdatedAt = now
		state.Nodes[node.ID] = node
	}
	var attributes map[string]any
	if reason != "" {
		attributes = map[string]any{"reason": reason}
	}
	store.AddAudit(state, s.ids, "lease", lease.ID, "released", now, attributes)
	return lease
}

func (s *Service) ExpireDue(ctx context.Context) (int, error) {
	now := s.clock.Now().UTC()
	expired := 0
	err := s.store.Write(ctx, func(state *domain.State) error {
		expired = expireLeases(state, now)
		return nil
	})
	return expired, err
}

func expireLeases(state *domain.State, now time.Time) int {
	count := 0
	for leaseID, lease := range state.Leases {
		if lease.State != domain.LeaseClaimed || lease.ExpiresAt.After(now) {
			continue
		}
		finishedAt := now
		lease.State = domain.LeaseExpired
		lease.FinishedAt = &finishedAt
		state.Leases[leaseID] = lease
		if node, ok := state.Nodes[lease.NodeID]; ok && node.State == domain.NodeLeased {
			node.State = domain.NodeReady
			node.Version++
			node.UpdatedAt = now
			state.Nodes[node.ID] = node
		}
		count++
	}
	return count
}
