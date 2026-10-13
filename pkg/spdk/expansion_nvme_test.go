package spdk

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func TestExpansionFeatureEncoding(t *testing.T) {
	for _, set := range []bool{false, true} {
		command, err := base64.URLEncoding.DecodeString(nvmeAECCommand(set, 0xa501))
		if err != nil || len(command) != 64 {
			t.Fatalf("invalid command: %v", err)
		}
		wantOpcode := byte(0x0a)
		if set {
			wantOpcode = 0x09
		}
		if command[0] != wantOpcode || binary.LittleEndian.Uint32(command[40:44]) != 0x0b {
			t.Fatalf("wrong feature command: %x", command)
		}
		if set && binary.LittleEndian.Uint32(command[44:48]) != 0xa501 {
			t.Fatal("feature value lost unrelated bits")
		}
	}
	var completion [16]byte
	binary.LittleEndian.PutUint32(completion[:4], 0xa501)
	// The completion phase bit is not an error status.
	completion[14] = 1
	value, err := nvmeCompletionValue(base64.URLEncoding.EncodeToString(completion[:]))
	if err != nil || value != 0xa501 {
		t.Fatalf("completion value=%x error=%v", value, err)
	}
	completion[14] = 5
	if _, err := nvmeCompletionValue(base64.URLEncoding.EncodeToString(completion[:])); err == nil {
		t.Fatal("NVMe error status accepted")
	}
	for _, invalid := range []string{"!", "AA==", base64.URLEncoding.EncodeToString(make([]byte, 17))} {
		if _, err := nvmeCompletionValue(invalid); err == nil {
			t.Fatalf("invalid completion accepted: %q", invalid)
		}
	}
}

func TestExpansionResetCleanup(t *testing.T) {
	for _, scenario := range []string{"success", "resize-error", "mask-error", "reset-error", "reset-changes-feature"} {
		t.Run(scenario, func(t *testing.T) {
			const original = uint32(0xa501)
			current := original
			var calls []string
			rpc := func(ctx context.Context, method string, params any, result any) error {
				if err := ctx.Err(); err != nil {
					t.Fatalf("cleanup used canceled context: %v", err)
				}
				if method == "bdev_nvme_reset_controller" {
					calls = append(calls, "reset")
					if scenario == "reset-error" {
						return fmt.Errorf("injected reset failure")
					}
					if scenario == "reset-changes-feature" {
						current = 0x100
					} else {
						current = original
					}
					*result.(*bool) = true
					return nil
				}
				if method != "bdev_nvme_send_cmd" {
					t.Fatalf("unexpected command: %s", method)
				}
				command, err := base64.URLEncoding.DecodeString(params.(map[string]any)["cmdbuf"].(string))
				if err != nil {
					return err
				}
				if command[0] == 9 {
					current = binary.LittleEndian.Uint32(command[44:48])
					calls = append(calls, "set")
					if scenario == "mask-error" && current == original & ^nvmeNamespaceNoticeBit {
						return fmt.Errorf("mask applied but response lost")
					}
				} else {
					calls = append(calls, "get")
				}
				var completion [16]byte
				binary.LittleEndian.PutUint32(completion[:4], current)
				data, err := json.Marshal(map[string]string{"cpl": base64.URLEncoding.EncodeToString(completion[:])})
				if err != nil {
					return err
				}
				return json.Unmarshal(data, result)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			resizeCalled := false
			resizeErr := errors.New("resize response lost after growth")
			err := resizeWithBackendRefresh(ctx, "replica", func() error {
				resizeCalled = true
				calls = append(calls, "resize")
				if current != original & ^nvmeNamespaceNoticeBit {
					t.Fatalf("resize with wrong notice mask: %x", current)
				}
				if scenario == "resize-error" {
					cancel()
					return resizeErr
				}
				return nil
			}, rpc)
			if !slices.Contains(calls, "reset") {
				t.Fatalf("missing reset cleanup: %v", calls)
			}
			if scenario == "reset-error" {
				if err == nil || !strings.Contains(err.Error(), "NOT confirmed") || current != original & ^nvmeNamespaceNoticeBit {
					t.Fatalf("unsafe reset-failure handling: value=%x error=%v", current, err)
				}
				return
			}
			if current != original {
				t.Fatalf("original feature not restored: %x", current)
			}
			switch scenario {
			case "resize-error":
				if !errors.Is(err, resizeErr) {
					t.Fatalf("lost resize error: %v", err)
				}
			case "mask-error":
				if err == nil || resizeCalled {
					t.Fatalf("resized without confirmed mask: %v", err)
				}
			default:
				if err != nil || !resizeCalled {
					t.Fatalf("resize failed: %v", err)
				}
			}
		})
	}
}

func TestExpansionResetFailureRetryUsesCapturedFeature(t *testing.T) {
	const original = uint32(0xa501)
	current := original
	resetCalls := 0
	resizeCalls := 0
	rpc := func(ctx context.Context, method string, params any, result any) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if method == "bdev_nvme_reset_controller" {
			resetCalls++
			if resetCalls == 1 {
				return fmt.Errorf("injected first reset failure")
			}
			current = original
			*result.(*bool) = true
			return nil
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
	captured, known, err := resizeWithBackendRefreshKnown(t.Context(), "replica", 0, false, func() error {
		resizeCalls++
		return nil
	}, rpc)
	if err == nil || !known || captured != original || current != original & ^nvmeNamespaceNoticeBit {
		t.Fatalf("first attempt did not retain uncertain state: original=%x known=%v current=%x error=%v", captured, known, current, err)
	}
	captured, known, err = resizeWithBackendRefreshKnown(t.Context(), "replica", captured, known, func() error {
		resizeCalls++
		return nil
	}, rpc)
	if err != nil || !known || captured != original || current != original || resetCalls != 3 || resizeCalls != 2 {
		t.Fatalf("retry did not repair from captured feature: original=%x known=%v current=%x resets=%d resizes=%d error=%v",
			captured, known, current, resetCalls, resizeCalls, err)
	}
}

func TestExpansionPendingCapacityBounds(t *testing.T) {
	e := &Engine{Name: "engine", SpecSize: 64, pendingExpansionSize: 128}
	for _, size := range []uint64{32, 64, 96, 128, 160} {
		bdev := &spdktypes.BdevInfo{BdevInfoBasic: spdktypes.BdevInfoBasic{Name: "replica", NumBlocks: size, BlockSize: 1}}
		err := validateReplicaBdevSize(e, "replica", bdev)
		wantError := size < 64 || size > 128
		if (err != nil) != wantError {
			t.Fatalf("size=%d error=%v", size, err)
		}
	}
	e.pendingExpansionSize = 0
	if err := validateReplicaBdevSize(e, "replica", &spdktypes.BdevInfo{BdevInfoBasic: spdktypes.BdevInfoBasic{NumBlocks: 96, BlockSize: 1}}); err == nil {
		t.Fatal("engine without a pending goal incorrectly accepts mismatched replica size")
	}
}

func TestExpansionFrontendlessEngineIsEligible(t *testing.T) {
	e := &Engine{Frontend: types.FrontendEmpty}
	if err := e.validateExpansionFrontendNoLock(); err != nil {
		t.Fatalf("idle frontendless engine rejected: %v", err)
	}
	e.NvmeTcpTarget = &NvmeTcpTarget{Port: 1}
	if err := e.validateExpansionFrontendNoLock(); err == nil {
		t.Fatal("frontendless engine with an active restore target accepted")
	}
}

func TestExpansionShardedDispatchRemainsSupported(t *testing.T) {
	engine := &Engine{
		Name: "sharded", SpecSize: 1 << 30,
		Frontend: types.FrontendSPDKTCPBlockdev,
		backends: map[string]Backend{"shard": newShardGroupBackend("shard", "", nil)},
	}
	server := &Server{engineMap: map[string]*Engine{engine.Name: engine}}
	// The unchanged shard-group refresh path can finish an already-grown request
	// without replica-head RPCs or the RAID1 frontend coordination predicate.
	if _, err := server.EngineExpand(t.Context(), &spdkrpc.EngineExpandRequest{Name: engine.Name, Size: engine.SpecSize}); err != nil {
		t.Fatalf("sharded expansion was routed through replica growth: %v", err)
	}
	engine.IsRestoring = true
	if _, err := server.EngineExpand(t.Context(), &spdkrpc.EngineExpandRequest{Name: engine.Name, Size: 2 << 30}); grpcstatus.Code(err) != grpccodes.FailedPrecondition {
		t.Fatalf("sharded restore guard did not reject before SPDK mutation: %v", err)
	}
}

func TestExpansionRejectsUnsupportedEngine(t *testing.T) {
	for _, e := range []*Engine{
		{isExpanding: true}, {IsRestoring: true}, {isCloning: true},
		{State: types.InstanceStateError}, {State: types.InstanceStateRunning},
	} {
		if _, err := e.precheckExpansionNoLock(t.Context(), 1<<30); err == nil {
			t.Fatal("unsupported state accepted")
		}
	}
}

func TestExpansionPartialGrowthRequiresRecordedGoal(t *testing.T) {
	e := &Engine{Name: "engine", SpecSize: 1 << 30, backends: map[string]Backend{}}
	for i, size := range []uint64{1 << 30, 2 << 30} {
		name := fmt.Sprintf("r%d", i)
		backend := newFakeBackend(name, "")
		backend.SetMode(types.ModeRW)
		backend.View = &BackendView{SpecSize: size}
		e.backends[name] = backend
	}
	if _, err := e.precheckBackendExpansionNoLock(2 << 30); err == nil {
		t.Fatal("unrecorded mixed-size growth accepted")
	}
	e.pendingExpansionSize = 2 << 30
	if required, err := e.precheckBackendExpansionNoLock(2 << 30); err != nil || !required {
		t.Fatalf("recorded partial growth rejected: required=%v error=%v", required, err)
	}
	if _, err := e.precheckBackendExpansionNoLock(3 << 30); err == nil {
		t.Fatal("new goal accepted while partial growth is pending")
	}
}
