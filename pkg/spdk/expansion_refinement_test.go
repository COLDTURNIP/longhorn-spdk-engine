package spdk

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/spdkrpc"
)

func TestExpansionDisconnectedExportCleanup(t *testing.T) {
	resizeErr := errors.New("resize response lost")
	restoreErr := errors.New("export restoration failed")
	stopErr := errors.New("export stop failed")
	for _, scenario := range []struct {
		name                  string
		stop, resize, restore error
		cancelResize          bool
	}{
		{name: "success"},
		{name: "resize-error", resize: resizeErr},
		{name: "resize-canceled", cancelResize: true},
		{name: "restore-error", restore: restoreErr},
		{name: "both-errors", resize: resizeErr, restore: restoreErr},
		{name: "stop-error", stop: stopErr},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var calls []string
			err := withStoppedExpansionExport(func() error {
				calls = append(calls, "stop")
				return scenario.stop
			}, func() error {
				calls = append(calls, "restore")
				if t.Context().Err() != nil {
					t.Fatal("cleanup inherited canceled resize context")
				}
				return scenario.restore
			}, func() error {
				calls = append(calls, "resize")
				if scenario.cancelResize {
					cancel()
					return ctx.Err()
				}
				return scenario.resize
			})
			wantCalls := []string{"stop", "resize", "restore"}
			if scenario.stop != nil {
				wantCalls = []string{"stop"}
			}
			if !slices.Equal(calls, wantCalls) {
				t.Fatalf("calls=%v want=%v", calls, wantCalls)
			}
			for _, wantErr := range []error{scenario.stop, scenario.resize, scenario.restore} {
				if wantErr != nil && !errors.Is(err, wantErr) {
					t.Fatalf("lost failure %v: %v", wantErr, err)
				}
			}
			if scenario.cancelResize && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost resize cancellation: %v", err)
			}
			if scenario.stop == nil && scenario.resize == nil && scenario.restore == nil && !scenario.cancelResize && err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestExpansionDisconnectedExportPreflight(t *testing.T) {
	for _, scenario := range []struct {
		name        string
		exposed     bool
		subsystems  string
		controllers string
		queryError  bool
		wantError   bool
	}{
		{name: "unexposed", subsystems: "[]"},
		{name: "unrelated-export", subsystems: `[{"nqn":"other"}]`},
		{name: "disconnected-export", exposed: true, subsystems: `[{"nqn":"replica"}]`, controllers: "[]"},
		{name: "connected-export", exposed: true, subsystems: `[{"nqn":"replica"}]`, controllers: "[{}]", wantError: true},
		{name: "stale-exposed-flag", subsystems: `[{"nqn":"replica"}]`, controllers: "[]", wantError: true},
		{name: "query-error", queryError: true, wantError: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var calls []string
			rpc := func(_ context.Context, method string, _ any, result any) error {
				calls = append(calls, method)
				if scenario.queryError {
					return errors.New("query failed")
				}
				switch method {
				case "nvmf_get_subsystems":
					return json.Unmarshal([]byte(scenario.subsystems), result)
				case "nvmf_subsystem_get_controllers":
					return json.Unmarshal([]byte(scenario.controllers), result)
				default:
					t.Fatalf("preflight mutated SPDK: %s", method)
					return nil
				}
			}
			err := requireDisconnectedExpansionExport(t.Context(), "replica", scenario.exposed, rpc)
			if (err != nil) != scenario.wantError {
				t.Fatalf("error=%v expected rejection=%v", err, scenario.wantError)
			}
			if scenario.name == "stale-exposed-flag" && (!strings.Contains(err.Error(), "residual") || len(calls) != 1) {
				t.Fatalf("stale state was not rejected before controller/resize work: %v calls=%v", err, calls)
			}
		})
	}
}

func TestExpansionLvolGrowthUsesObservedCapacity(t *testing.T) {
	const mib = uint64(1 << 20)
	resizeErr := errors.New("resize response lost")
	for _, scenario := range []struct {
		name       string
		initial    uint64
		grown      uint64
		resizeErr  error
		ack        bool
		wantResize bool
		wantError  bool
		cancel     bool
	}{
		{name: "growth", initial: mib, grown: 2 * mib, ack: true, wantResize: true},
		{name: "already-grown", initial: 2 * mib},
		{name: "never-shrink", initial: 3 * mib, wantError: true},
		{name: "lost-response-after-growth", initial: mib, grown: 2 * mib, resizeErr: resizeErr, wantResize: true},
		{name: "false-response-after-growth", initial: mib, grown: 2 * mib, wantResize: true},
		{name: "oversized-growth", initial: mib, grown: 3 * mib, wantResize: true, wantError: true},
		{name: "incomplete-canceled-growth", initial: mib, grown: mib, resizeErr: resizeErr, wantResize: true, wantError: true, cancel: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			actual := scenario.initial
			resizeCalls := 0
			rpc := func(_ context.Context, method string, params any, result any) error {
				if params.(map[string]any)["name"] != "disk/replica" {
					t.Fatal("wrong bdev identity")
				}
				switch method {
				case "bdev_get_bdevs":
					data, err := json.Marshal([]map[string]uint64{{"num_blocks": actual / 512, "block_size": 512}})
					if err != nil {
						return err
					}
					return json.Unmarshal(data, result)
				case "bdev_lvol_resize":
					resizeCalls++
					if params.(map[string]any)["size_in_mib"] != uint64(2) {
						t.Fatal("wrong resize goal")
					}
					actual = scenario.grown
					*result.(*bool) = scenario.ack
					if scenario.cancel {
						cancel()
					}
					return scenario.resizeErr
				default:
					t.Fatalf("unexpected RPC: %s", method)
					return nil
				}
			}
			err := resizeExpansionLvol(ctx, "disk/replica", 2*mib, rpc)
			if (err != nil) != scenario.wantError || (resizeCalls == 1) != scenario.wantResize {
				t.Fatalf("error=%v resizeCalls=%d", err, resizeCalls)
			}
			if scenario.cancel && (!errors.Is(err, resizeErr) || !errors.Is(err, context.Canceled)) {
				t.Fatalf("lost partial-growth retry errors: %v", err)
			}
		})
	}
}

func TestExpansionPrecheckHonorsRequestCancellation(t *testing.T) {
	for _, scenario := range []struct {
		name string
		ctx  func(context.Context) (context.Context, context.CancelFunc)
		code codes.Code
	}{
		{name: "canceled", ctx: func(parent context.Context) (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(parent)
			cancel()
			return ctx, cancel
		}, code: codes.Canceled},
		{name: "expired", ctx: func(parent context.Context) (context.Context, context.CancelFunc) {
			return context.WithTimeout(parent, 0)
		}, code: codes.DeadlineExceeded},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx, cancel := scenario.ctx(t.Context())
			defer cancel()
			e := &Engine{Name: "engine", SpecSize: 1 << 30}
			s := &Server{engineMap: map[string]*Engine{e.Name: e}}
			resp, err := s.EngineExpandPrecheck(ctx, &spdkrpc.EngineExpandPrecheckRequest{Name: e.Name, Size: 2 << 30})
			if status.Code(err) != scenario.code || resp.ExpansionRequired || e.isExpanding || e.pendingExpansionSize != 0 || e.SpecSize != 1<<30 {
				t.Fatalf("canceled precheck did work: response=%v error=%v engine=%+v", resp, err, e)
			}
		})
	}
}
