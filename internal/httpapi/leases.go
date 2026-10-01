package httpapi

import (
	"io"
	"net/http"

	"solo-0014-zone-rollout-control/internal/domain"
)

func (s *Server) allocateLeases(w http.ResponseWriter, r *http.Request) {
	var input domain.LeaseInput
	if err := decode(w, r, &input); err != nil {
		sendError(w, r, err)
		return
	}
	group, err := s.allocations.Allocate(r.Context(), r.PathValue("id"), input)
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusCreated, group)
}

func (s *Server) releaseLease(w http.ResponseWriter, r *http.Request) {
	lease, err := s.allocations.Release(r.Context(), r.PathValue("id"))
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusOK, lease)
}

func (s *Server) releaseLeaseGroup(w http.ResponseWriter, r *http.Request) {
	var input domain.LeaseGroupReleaseInput
	// The release reason is optional: an absent or empty body is accepted.
	if r.ContentLength != 0 {
		payload, err := io.ReadAll(io.LimitReader(r.Body, requestLimit))
		if err != nil {
			sendError(w, r, domain.Validation("request body could not be read", map[string]any{"reason": err.Error()}))
			return
		}
		if len(payload) > 0 {
			if err := decodeJSON(payload, &input); err != nil {
				sendError(w, r, err)
				return
			}
		}
	}
	result, err := s.allocations.ReleaseGroup(r.Context(), r.PathValue("id"), input)
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusOK, result)
}
