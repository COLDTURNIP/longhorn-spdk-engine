package spdk

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func TestExpansionPreservesExternalFrontendSuspension(t *testing.T) {
	for _, frontend := range []string{types.FrontendEmpty, types.FrontendSPDKTCPBlockdev} {
		for _, size := range []uint64{1 << 30, 2 << 30} {
			t.Run(fmt.Sprintf("%s/%d", frontend, size), func(t *testing.T) {
				clientCreated := false
				updates := make(chan any, 1)
				ef := NewEngineFrontend("frontend", "engine", "volume", frontend, 1<<30, 0, 0, updates,
					func(string) (*client.SPDKClient, error) {
						clientCreated = true
						return nil, errors.New("expansion contacted the engine while externally suspended")
					})
				ef.State = types.InstanceStateSuspended
				ef.ErrorMsg = "existing lifecycle status"
				ef.lastExpansionError = "previous expansion status"
				ef.lastExpansionFailedAt = "previous timestamp"
				server := &Server{engineFrontendMap: map[string]*EngineFrontend{ef.Name: ef}}

				_, err := server.EngineFrontendExpand(t.Context(), &spdkrpc.EngineFrontendExpandRequest{Name: ef.Name, Size: size})
				if status.Code(err) != codes.FailedPrecondition || clientCreated {
					t.Fatalf("externally suspended expansion was not rejected before client creation: error=%v clientCreated=%v", err, clientCreated)
				}
				if ef.State != types.InstanceStateSuspended || ef.SpecSize != 1<<30 || ef.isExpanding || len(updates) != 0 ||
					ef.ErrorMsg != "existing lifecycle status" || ef.lastExpansionError != "previous expansion status" || ef.lastExpansionFailedAt != "previous timestamp" {
					t.Fatalf("rejected expansion changed frontend state: %+v", ef)
				}
			})
		}
	}
}
