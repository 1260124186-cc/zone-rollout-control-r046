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
		node, ok := state.Nodes[lease.NodeID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "lease references a missing node", map[string]any{
				"node_id": lease.NodeID,
			})
		}
		lease = applyClaimRelease(state, lease, node, now)
		store.AddAudit(state, s.ids, "lease", leaseID, "released", now, nil)
		result = lease
		return nil
	})
	return result, err
}

// ReleaseGroup releases every claimed lease that belongs to one allocation
// group within a single transaction. All group lease references and their
// nodes are validated before any state change, so a broken reference aborts
// the whole batch rather than releasing only part of the group. Leases that
// already reached a terminal state are reported per item without touching
// nodes or audit; replaying the same group release is therefore a no-op.
func (s *Service) ReleaseGroup(
	ctx context.Context,
	groupID string,
	input domain.LeaseGroupReleaseInput,
) (domain.LeaseGroupRelease, error) {
	input = domain.NormalizeLeaseGroupReleaseInput(input)
	if err := domain.ValidateLeaseGroupReleaseInput(input); err != nil {
		return domain.LeaseGroupRelease{}, err
	}
	now := s.clock.Now().UTC()
	var result domain.LeaseGroupRelease
	err := s.store.Write(ctx, func(state *domain.State) error {
		group, ok := state.LeaseGroups[groupID]
		if !ok {
			return domain.NotFound("lease_group", groupID)
		}

		leases := make([]domain.NodeLease, len(group.LeaseIDs))
		nodes := make([]domain.ResolverNode, len(group.LeaseIDs))
		for index, leaseID := range group.LeaseIDs {
			lease, exists := state.Leases[leaseID]
			if !exists {
				return domain.NewError(domain.CodeStorageFailure, "lease group references a missing lease", map[string]any{
					"group_id": groupID,
					"lease_id": leaseID,
				})
			}
			if lease.AllocationID != groupID {
				return domain.NewError(domain.CodeStorageFailure, "lease references a different allocation group", map[string]any{
					"group_id":            groupID,
					"lease_id":            leaseID,
					"lease_allocation_id": lease.AllocationID,
				})
			}
			leases[index] = lease
			if lease.State == domain.LeaseClaimed {
				node, exists := state.Nodes[lease.NodeID]
				if !exists {
					return domain.NewError(domain.CodeStorageFailure, "lease references a missing node", map[string]any{
						"group_id": groupID,
						"lease_id": leaseID,
						"node_id":  lease.NodeID,
					})
				}
				nodes[index] = node
			}
		}

		items := make([]domain.LeaseReleaseItem, 0, len(leases))
		releasedCount := 0
		for index, lease := range leases {
			outcome := releaseOutcome(lease.State)
			if lease.State == domain.LeaseClaimed {
				lease = applyClaimRelease(state, lease, nodes[index], now)
				attributes := map[string]any{
					"group_id": groupID,
					"plan_id":  lease.PlanID,
				}
				if input.Reason != "" {
					attributes["reason"] = input.Reason
				}
				store.AddAudit(state, s.ids, "lease", lease.ID, "released", now, attributes)
				outcome = domain.LeaseReleaseReleased
				releasedCount++
			}
			items = append(items, domain.LeaseReleaseItem{
				Lease:   lease,
				Outcome: outcome,
			})
		}
		if releasedCount > 0 {
			attributes := map[string]any{
				"plan_id":        group.PlanID,
				"lease_count":    len(group.LeaseIDs),
				"released_count": releasedCount,
			}
			if input.Reason != "" {
				attributes["reason"] = input.Reason
			}
			store.AddAudit(state, s.ids, "lease_group", groupID, "released", now, attributes)
		}
		result = domain.LeaseGroupRelease{
			GroupID:       groupID,
			PlanID:        group.PlanID,
			Reason:        input.Reason,
			ReleasedCount: releasedCount,
			Results:       items,
		}
		return nil
	})
	return result, err
}

func releaseOutcome(state domain.LeaseState) domain.LeaseReleaseOutcome {
	switch state {
	case domain.LeaseReleased:
		return domain.LeaseReleaseAlreadyReleased
	case domain.LeaseReported:
		return domain.LeaseReleaseAlreadyReported
	case domain.LeaseExpired:
		return domain.LeaseReleaseAlreadyExpired
	default:
		return domain.LeaseReleaseAlreadyReleased
	}
}

// applyClaimRelease moves one claimed lease to released and returns its node
// to ready, applying the same terminal rule used by the single-lease release.
// The caller must have confirmed the lease is claimed and the node exists.
func applyClaimRelease(
	state *domain.State,
	lease domain.NodeLease,
	node domain.ResolverNode,
	now time.Time,
) domain.NodeLease {
	finishedAt := now
	lease.State = domain.LeaseReleased
	lease.FinishedAt = &finishedAt
	state.Leases[lease.ID] = lease
	if node.State == domain.NodeLeased {
		node.State = domain.NodeReady
		node.Version++
		node.UpdatedAt = now
		state.Nodes[node.ID] = node
	}
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
