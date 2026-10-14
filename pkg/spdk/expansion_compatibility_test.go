package spdk

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"

	"google.golang.org/grpc/metadata"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func TestExpansionDegradedCompletion(t *testing.T) {
	resizeError := fmt.Errorf("%w: permanent resize failure", errExpansionReplicaResize)
	for _, scenario := range []struct {
		name                  string
		failures              map[string]error
		excludeError          error
		wantGrow, wantExclude []string
		wantError             bool
	}{
		{name: "healthy", wantGrow: []string{"a", "b", "c"}},
		{name: "first fails", failures: map[string]error{"a": resizeError}, wantGrow: []string{"a", "b", "c"}, wantExclude: []string{"a"}},
		{name: "last fails", failures: map[string]error{"c": resizeError}, wantGrow: []string{"a", "b", "c"}, wantExclude: []string{"c"}},
		{name: "all fail", failures: map[string]error{"a": resizeError, "b": resizeError, "c": resizeError}, wantGrow: []string{"a", "b", "c"}, wantError: true},
		{name: "restore uncertain", failures: map[string]error{"a": errors.Join(resizeError, errExpansionBackendUnsafe)}, wantGrow: []string{"a"}, wantError: true},
		{name: "mask unconfirmed", failures: map[string]error{"a": errors.New("mask failure")}, wantGrow: []string{"a"}, wantError: true},
		{name: "removal uncertain", failures: map[string]error{"a": resizeError}, excludeError: errors.New("removal lost"), wantGrow: []string{"a", "b", "c"}, wantExclude: []string{"a"}, wantError: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var grown, excluded []string
			_, err := expandReplicaBackends(t.Context(), []string{"a", "b", "c"}, func(name string) error {
				grown = append(grown, name)
				return scenario.failures[name]
			}, func(name string) error {
				excluded = append(excluded, name)
				return scenario.excludeError
			})
			if (err != nil) != scenario.wantError || !slices.Equal(grown, scenario.wantGrow) || !slices.Equal(excluded, scenario.wantExclude) {
				t.Fatalf("error=%v grown=%v excluded=%v", err, grown, excluded)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	_, err := expandReplicaBackends(ctx, []string{"a", "b"}, func(string) error {
		cancel()
		return resizeError
	}, func(string) error { t.Fatal("excluded a member after cancellation"); return nil })
	if err == nil {
		t.Fatal("canceled expansion succeeded")
	}
}

func TestExpansionRemovalUsesObservedMembership(t *testing.T) {
	for _, scenario := range []struct {
		name        string
		present     bool
		ack         bool
		removeError error
		invalid     bool
		wantError   bool
	}{
		{name: "removed", ack: true},
		{name: "response lost", removeError: errors.New("lost")},
		{name: "false response"},
		{name: "still present", present: true, wantError: true},
		{name: "unknown RAID", invalid: true, wantError: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			calls := 0
			err := removeExpansionRaidMember(t.Context(), "engine", "failedn1", func(_ context.Context, method string, params, result any) error {
				calls++
				switch method {
				case "bdev_raid_remove_base_bdev":
					if params.(map[string]any)["name"] != "failedn1" {
						t.Fatal("wrong member")
					}
					*result.(*bool) = scenario.ack
					return scenario.removeError
				case "bdev_get_bdevs":
					if params.(map[string]any)["name"] != "engine" {
						t.Fatal("wrong RAID")
					}
					members := []map[string]string{{"name": "healthyn1"}}
					if scenario.present {
						members = append(members, map[string]string{"name": "failedn1"})
					}
					var value any = []any{map[string]any{"driver_specific": map[string]any{"raid": map[string]any{"base_bdevs_list": members}}}}
					if scenario.invalid {
						value = []any{}
					}
					data, err := json.Marshal(value)
					if err != nil {
						return err
					}
					return json.Unmarshal(data, result)
				default:
					t.Fatalf("unexpected destructive RPC %s", method)
					return nil
				}
			})
			if (err != nil) != scenario.wantError || calls != 2 {
				t.Fatalf("error=%v calls=%d", err, calls)
			}
		})
	}
}

func TestExpansionNvmfRetainsOnlineCompatibility(t *testing.T) {
	e := &Engine{Frontend: types.FrontendSPDKTCPNvmf, NvmeTcpTarget: &NvmeTcpTarget{ANAState: NvmeTCPANAStateOptimized}}
	if err := e.validateExpansionFrontendNoLock(); err != nil {
		t.Fatal(err)
	}
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(client.ExpansionPhaseMetadataKey, "frontend-nvmf"))
	if err := e.validateExpansionCoordinationNoLock(ctx); err != nil {
		t.Fatal(err)
	}
	ctx = metadata.NewIncomingContext(t.Context(), metadata.Pairs(client.ExpansionPhaseMetadataKey, "frontend-suspended"))
	if err := e.validateExpansionCoordinationNoLock(ctx); err == nil {
		t.Fatal("NVMf falsely claimed a local suspension")
	}
	ef := &EngineFrontend{Frontend: types.FrontendSPDKTCPNvmf, Endpoint: "nvmf://existing", NvmeTcpFrontend: &NvmeTcpFrontend{TargetIP: "storage", TargetPort: 1234}}
	if err := ef.validateExpansionFrontendNoLock(); err != nil {
		t.Fatal(err)
	}
	if ef.initiator != nil {
		t.Fatal("created a local dm initiator for external consumers")
	}
}

func TestExpansionConfirmedRefusalReturnsBeforeDeadline(t *testing.T) {
	const mib = uint64(1 << 20)
	for _, refusal := range []error{nil, &jsonrpc.ResponseError{Code: -32603, Message: "growth refused"}} {
		calls := 0
		err := resizeExpansionLvol(t.Context(), "disk/head", 2*mib, func(_ context.Context, method string, _ any, result any) error {
			calls++
			switch method {
			case "bdev_get_bdevs":
				return json.Unmarshal([]byte("[{\"num_blocks\":2048,\"block_size\":512}]"), result)
			case "bdev_lvol_resize":
				*result.(*bool) = false
				return refusal
			default:
				t.Fatalf("unexpected RPC %s", method)
				return nil
			}
		})
		if !errors.Is(err, errExpansionReplicaResize) || calls != 3 {
			t.Fatalf("refusal consumed deadline or lost classification: error=%v calls=%d", err, calls)
		}
	}
}

func TestExpansionDeferredRefreshTransfersOnlySuccessfulGrowth(t *testing.T) {
	for _, resizeError := range []error{nil, errors.New("uncertain growth")} {
		const original = uint32(0xa501)
		current, resets := original, 0
		rpc := func(_ context.Context, method string, params, result any) error {
			if method == "bdev_nvme_reset_controller" {
				resets++
				*result.(*bool) = true
				return nil
			}
			if method != "bdev_nvme_send_cmd" {
				t.Fatalf("unexpected RPC %s", method)
			}
			command, err := base64.URLEncoding.DecodeString(params.(map[string]any)["cmdbuf"].(string))
			if err != nil {
				return err
			}
			if command[0] == 9 {
				current = binary.LittleEndian.Uint32(command[44:48])
			}
			var completion [16]byte
			binary.LittleEndian.PutUint32(completion[:4], current)
			data, err := json.Marshal(map[string]string{"cpl": base64.URLEncoding.EncodeToString(completion[:])})
			if err != nil {
				return err
			}
			return json.Unmarshal(data, result)
		}
		held := false
		value, captured, err := resizeWithDeferredBackendRefresh(t.Context(), "healthy", 0, false,
			func() error { return resizeError }, rpc, func() bool { held = true; return true })
		if value != original || !captured || (err != nil) != (resizeError != nil) || held != (resizeError == nil) {
			t.Fatalf("original=%x captured=%v held=%v error=%v", value, captured, held, err)
		}
		if resizeError == nil {
			if resets != 0 || current != original & ^nvmeNamespaceNoticeBit {
				t.Fatal("backend refreshed before failed-member exclusion")
			}
			if err := resetAndRestoreBackend(t.Context(), "healthy", value, rpc); err != nil {
				t.Fatal(err)
			}
		}
		if resets != 1 || current != original {
			t.Fatal("backend ownership did not end with confirmed restoration")
		}
	}
}
