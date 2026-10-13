package spdk

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/metadata"

	"github.com/longhorn/go-spdk-helper/pkg/initiator"

	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func TestExpansionFrontendlessCoordination(t *testing.T) {
	for _, frontend := range []string{types.FrontendEmpty, types.FrontendSPDKTCPBlockdev} {
		e := &Engine{Frontend: frontend}
		want := "frontend-suspended"
		wrong := "backend-idle"
		if frontend == types.FrontendEmpty {
			want, wrong = wrong, want
		}
		for _, phase := range []string{"", wrong, want} {
			ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(client.ExpansionPhaseMetadataKey, phase))
			err := e.validateExpansionCoordinationNoLock(ctx)
			if (err == nil) != (phase == want) {
				t.Fatalf("frontend=%q phase=%q error=%v", frontend, phase, err)
			}
		}
	}
}

func TestExpansionFrontendlessRestoreResidue(t *testing.T) {
	for _, target := range []*NvmeTcpTarget{nil, {}, {Port: 1}, {Nqn: "restore"}, {IP: "restore-target"}} {
		e := &Engine{Frontend: types.FrontendEmpty, NvmeTcpTarget: target}
		wantError := target != nil && (target.Port != 0 || target.Nqn != "" || target.IP != "")
		if err := e.validateExpansionFrontendNoLock(); (err != nil) != wantError {
			t.Fatalf("target=%+v error=%v", target, err)
		}
	}
	for _, ef := range []*EngineFrontend{
		{}, {IsRestoring: true}, {Endpoint: "restore-endpoint"},
		{initiator: &initiator.Initiator{}}, {NvmeTcpFrontend: &NvmeTcpFrontend{TargetPort: 1}},
		{NvmeTcpFrontend: &NvmeTcpFrontend{TargetIP: "restore-target"}},
		{NvmeTcpFrontend: &NvmeTcpFrontend{Nqn: "restore"}},
	} {
		wantError := ef.IsRestoring || ef.Endpoint != "" || ef.initiator != nil || ef.NvmeTcpFrontend != nil
		if err := ef.validateExpansionFrontendNoLock(); (err != nil) != wantError {
			t.Fatalf("frontend residue accepted=%v error=%v", wantError, err)
		}
	}
}

func TestExpansionOnlineSafetyRemainsRequired(t *testing.T) {
	for _, e := range []*Engine{
		{Frontend: types.FrontendSPDKTCPBlockdev},
		{Frontend: types.FrontendSPDKTCPBlockdev, NvmeTcpTarget: &NvmeTcpTarget{ANAState: NvmeTCPANAStateNonOptimized}},
		{Frontend: types.FrontendSPDKTCPNvmf},
	} {
		if err := e.validateExpansionFrontendNoLock(); err == nil {
			t.Fatal("unsupported online engine accepted")
		}
	}
	e := &Engine{Frontend: types.FrontendSPDKTCPBlockdev, NvmeTcpTarget: &NvmeTcpTarget{ANAState: NvmeTCPANAStateOptimized}}
	if err := e.validateExpansionFrontendNoLock(); err != nil {
		t.Fatal(err)
	}
	for _, ef := range []*EngineFrontend{
		{Frontend: types.FrontendSPDKTCPBlockdev},
		{Frontend: types.FrontendSPDKTCPBlockdev, Endpoint: "endpoint"},
		{Frontend: types.FrontendSPDKTCPBlockdev, initiator: &initiator.Initiator{}},
	} {
		if err := ef.validateExpansionFrontendNoLock(); err == nil {
			t.Fatal("online frontend without both dm initiator and endpoint accepted")
		}
	}
}

func TestExpansionRestoreCannotStartDuringExpansion(t *testing.T) {
	ef := NewEngineFrontend("frontend", "engine", "volume", types.FrontendEmpty, 1<<30, 0, 0, make(chan interface{}, 1), nil)
	ef.isExpanding = true
	_, err := ef.BackupRestore(nil, nil, "unused", nil, 1, nil)
	if !errors.Is(err, ErrExpansionInProgress) || ef.IsRestoring || ef.Frontend != types.FrontendEmpty {
		t.Fatalf("restore started during expansion: error=%v restoring=%v frontend=%q", err, ef.IsRestoring, ef.Frontend)
	}
}

func TestExpansionFrontendlessBusyEngineRejectedBeforeRPC(t *testing.T) {
	for _, e := range []*Engine{
		{State: types.InstanceStateRunning, IsRestoring: true},
		{State: types.InstanceStateRunning, isExpanding: true},
		{State: types.InstanceStateRunning, isCloning: true},
		{State: types.InstanceStateRunning, backends: map[string]Backend{"replica": newReplicaBackend("replica", "", nil)}},
	} {
		if _, err := e.precheckExpansionNoLock(t.Context(), 2<<30); err == nil {
			t.Fatal("busy or non-RW frontendless engine accepted")
		}
	}
}

func TestExpansionBackendFlushCommand(t *testing.T) {
	for _, scenario := range []string{"success", "rpc-error", "nvme-error", "bad-completion"} {
		t.Run(scenario, func(t *testing.T) {
			rpc := func(ctx context.Context, method string, params any, result any) error {
				if ctx.Err() != nil || method != "bdev_nvme_send_cmd" {
					t.Fatalf("unexpected flush RPC %s", method)
				}
				p := params.(map[string]any)
				cmd, err := base64.URLEncoding.DecodeString(p["cmdbuf"].(string))
				if err != nil || len(cmd) != 64 || cmd[0] != 0 || binary.LittleEndian.Uint32(cmd[4:8]) != 1 || p["cmd_type"] != "io" || p["data_len"] != 0 || p["name"] != "replica" {
					t.Fatal("wrong NVMe flush encoding")
				}
				if scenario == "rpc-error" {
					return fmt.Errorf("injected flush failure")
				}
				var completion [16]byte
				if scenario == "nvme-error" {
					completion[14] = 2
				}
				encoded := base64.URLEncoding.EncodeToString(completion[:])
				if scenario == "bad-completion" {
					encoded = "!"
				}
				data, err := json.Marshal(map[string]string{"cpl": encoded})
				if err != nil {
					return err
				}
				return json.Unmarshal(data, result)
			}
			if err := flushExpansionBackend(t.Context(), "replica", rpc); (err == nil) != (scenario == "success") {
				t.Fatalf("scenario=%s error=%v", scenario, err)
			}
		})
	}
}

func TestExpansionFrontendlessRequiresActualExportRemoval(t *testing.T) {
	for _, scenario := range []string{"removed", "other-volume", "still-exported", "rpc-error"} {
		t.Run(scenario, func(t *testing.T) {
			rpc := func(ctx context.Context, method string, _ any, result any) error {
				if ctx.Err() != nil || method != "nvmf_get_subsystems" {
					t.Fatal("wrong export verification RPC")
				}
				if scenario == "rpc-error" {
					return fmt.Errorf("injected export query failure")
				}
				data := "[]"
				if scenario == "other-volume" {
					data = `[{"nqn":"other-volume"}]`
				}
				if scenario == "still-exported" {
					data = `[{"nqn":"restore"}]`
				}
				return json.Unmarshal([]byte(data), result)
			}
			err := requireNoExpansionExport(t.Context(), "restore", rpc)
			wantError := scenario == "still-exported" || scenario == "rpc-error"
			if (err != nil) != wantError {
				t.Fatalf("scenario=%s error=%v", scenario, err)
			}
		})
	}
}
