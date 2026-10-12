package spdk

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"time"

	"google.golang.org/grpc/metadata"

	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
)

const (
	nvmeNamespaceNoticeBit = uint32(1 << 8)
	expansionRPCDeadline   = 10 * time.Second
	expansionTimeout       = 90 * time.Second
)

func hasExpansionPhase(ctx context.Context, value string) bool {
	return slices.Contains(metadata.ValueFromIncomingContext(ctx, client.ExpansionPhaseMetadataKey), value)
}

func expansionPhase(ctx context.Context) (string, error) {
	phases := metadata.ValueFromIncomingContext(ctx, client.ExpansionPhaseMetadataKey)
	if len(phases) == 0 {
		return "", nil
	}
	if len(phases) != 1 {
		return "", fmt.Errorf("expansion requires a single coordination phase")
	}
	switch phases[0] {
	case "frontend-suspended", "backend-idle", "replica-precheck", "backend-masked":
		return phases[0], nil
	default:
		return "", fmt.Errorf("unknown expansion coordination phase %q", phases[0])
	}
}

// expansionRPC uses a dedicated, deadline-bound connection. No shared helper client or
// process-wide timeout is modified, and no generic passthrough RPC is exported.
func expansionRPC(ctx context.Context, method string, params any, result any) error {
	ctx, cancel := context.WithTimeout(ctx, expansionRPCDeadline)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, helpertypes.DefaultJSONServerNetwork, helpertypes.DefaultUnixDomainSocketPath)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	request := struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Method  string `json:"method"`
		Params  any    `json:"params"`
	}{"2.0", 1, method, params}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return err
	}
	var response struct {
		ID     int             `json:"id"`
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		return fmt.Errorf("%s: %w", method, err)
	}
	if response.ID != 1 || (len(response.Error) != 0 && string(response.Error) != "null") {
		return fmt.Errorf("%s response id=%d error=%s", method, response.ID, response.Error)
	}
	return json.Unmarshal(response.Result, result)
}

// Restore cleanup clears in-memory target fields even when teardown reports an
// error. Check SPDK too before treating a frontendless engine as unexported.
func requireNoExpansionExport(ctx context.Context, nqn string, rpc func(context.Context, string, any, any) error) error {
	var subsystems []struct {
		NQN string `json:"nqn"`
	}
	if err := rpc(ctx, "nvmf_get_subsystems", map[string]any{}, &subsystems); err != nil {
		return fmt.Errorf("cannot confirm stopped restore target: %w", err)
	}
	for _, subsystem := range subsystems {
		if subsystem.NQN == nqn {
			return fmt.Errorf("frontendless expansion requires the temporary restore export to be removed")
		}
	}
	return nil
}

// Replica exports have one namespace (NSID 1). Flush each SPDK backend after
// frontend quiescing, or after confirming an idle frontendless DR engine, and
// before any resize. This does not create or reset a Linux NVMe controller.
func flushExpansionBackend(ctx context.Context, controller string, rpc func(context.Context, string, any, any) error) error {
	var command [64]byte
	command[0] = 0x00 // NVMe NVM Flush.
	binary.LittleEndian.PutUint32(command[4:8], 1)
	var response struct {
		Completion string `json:"cpl"`
	}
	if err := rpc(ctx, "bdev_nvme_send_cmd", map[string]any{
		"name": controller, "cmd_type": "io", "data_direction": "c2h",
		"data_len": 0, "timeout_ms": 5000, "cmdbuf": base64.URLEncoding.EncodeToString(command[:]),
	}, &response); err != nil {
		return fmt.Errorf("backend %s flush failed: %w", controller, err)
	}
	if _, err := nvmeCompletionValue(response.Completion); err != nil {
		return fmt.Errorf("backend %s flush completion failed: %w", controller, err)
	}
	return nil
}

func nvmeAECCommand(set bool, value uint32) string {
	var command [64]byte
	command[0] = 0x0a // Get Features, current value.
	if set {
		command[0] = 0x09 // Set Features, do not persist across power cycles.
		binary.LittleEndian.PutUint32(command[44:48], value)
	}
	binary.LittleEndian.PutUint32(command[40:44], 0x0b) // Async Event Configuration.
	return base64.URLEncoding.EncodeToString(command[:])
}

func nvmeCompletionValue(encoded string) (uint32, error) {
	completion, err := base64.URLEncoding.DecodeString(encoded)
	if err != nil || len(completion) != 16 {
		return 0, fmt.Errorf("invalid NVMe completion %q: %v", encoded, err)
	}
	status := (binary.LittleEndian.Uint16(completion[14:16]) >> 1) & 0x7ff
	if status != 0 {
		return 0, fmt.Errorf("NVMe feature command failed: status %#x", status)
	}
	return binary.LittleEndian.Uint32(completion[:4]), nil
}

func nvmeAECFeature(ctx context.Context, controller string, set bool, value uint32, rpc func(context.Context, string, any, any) error) (uint32, error) {
	var response struct {
		Completion string `json:"cpl"`
	}
	err := rpc(ctx, "bdev_nvme_send_cmd", map[string]any{
		"name": controller, "cmd_type": "admin", "data_direction": "c2h",
		"data_len": 0, "timeout_ms": 5000, "cmdbuf": nvmeAECCommand(set, value),
	}, &response)
	if err != nil {
		return 0, err
	}
	return nvmeCompletionValue(response.Completion)
}

func expansionBdevCapacity(ctx context.Context, name string) (uint64, error) {
	var bdevs []struct {
		NumBlocks uint64 `json:"num_blocks"`
		BlockSize uint64 `json:"block_size"`
	}
	if err := expansionRPC(ctx, "bdev_get_bdevs", map[string]any{"name": name}, &bdevs); err != nil {
		return 0, err
	}
	if len(bdevs) != 1 || bdevs[0].BlockSize == 0 {
		return 0, fmt.Errorf("expected one bdev with nonzero block size: %s", name)
	}
	return bdevs[0].NumBlocks * bdevs[0].BlockSize, nil
}

func waitExpansionBdevCapacity(ctx context.Context, name string, size uint64) error {
	for {
		actual, err := expansionBdevCapacity(ctx, name)
		if err != nil {
			return err
		}
		if actual == size {
			return nil
		}
		if actual > size {
			return fmt.Errorf("bdev %s size %d exceeds goal %d", name, actual, size)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("bdev %s size %d has not reached %d: %w", name, actual, size, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// resetAndRestoreBackend refreshes backend capacity, then restores and verifies the
// feature value captured before the expansion attempt.
func resetAndRestoreBackend(ctx context.Context, controller string, original uint32, rpc func(context.Context, string, any, any) error) error {
	var reset bool
	if err := rpc(ctx, "bdev_nvme_reset_controller", map[string]any{"name": controller}, &reset); err != nil || !reset {
		return fmt.Errorf("backend %s reset failed; notice restoration NOT confirmed: reset=%v error=%v", controller, reset, err)
	}
	current, err := nvmeAECFeature(ctx, controller, false, 0, rpc)
	if err == nil && current != original {
		_, err = nvmeAECFeature(ctx, controller, true, original, rpc)
	}
	if err == nil {
		current, err = nvmeAECFeature(ctx, controller, false, 0, rpc)
	}
	if err != nil || current != original {
		return fmt.Errorf("backend %s notice restoration failed: got=%#x want=%#x error=%v", controller, current, original, err)
	}
	return nil
}

// resizeWithBackendRefresh always tries to refresh after a resize attempt,
// including an RPC timeout: the lvol may already have grown. Never guess the
// original feature value after a failed reset; the engine retains it for
// in-process retry.

func resizeWithBackendRefresh(ctx context.Context, controller string, resize func() error, rpc func(context.Context, string, any, any) error) error {
	_, _, err := resizeWithBackendRefreshKnown(ctx, controller, 0, false, resize, rpc)
	return err
}

func resizeWithBackendRefreshKnown(ctx context.Context, controller string, knownOriginal uint32, hasKnownOriginal bool, resize func() error, rpc func(context.Context, string, any, any) error) (original uint32, captured bool, retErr error) {
	original, captured = knownOriginal, hasKnownOriginal
	current, err := nvmeAECFeature(ctx, controller, false, 0, rpc)
	if err != nil {
		return original, captured, err
	}
	if !captured {
		original, captured = current, true
	} else if current != original {
		if err := resetAndRestoreBackend(ctx, controller, original, rpc); err != nil {
			return original, captured, fmt.Errorf("backend %s prior feature state repair failed: %w", controller, err)
		}
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*expansionRPCDeadline)
		defer cancel()
		retErr = errors.Join(retErr, resetAndRestoreBackend(cleanup, controller, original, rpc))
	}()
	if _, err := nvmeAECFeature(ctx, controller, true, original & ^nvmeNamespaceNoticeBit, rpc); err != nil {
		return original, captured, err
	}
	masked, err := nvmeAECFeature(ctx, controller, false, 0, rpc)
	if err != nil || masked != original & ^nvmeNamespaceNoticeBit {
		return original, captured, fmt.Errorf("backend %s notice mask not confirmed: got=%#x error=%v", controller, masked, err)
	}
	return original, captured, resize()
}
