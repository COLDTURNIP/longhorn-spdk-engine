package spdk

import (
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func expansionCloneFixture(clone, source bool) (*Replica, map[string]*spdktypes.BdevInfo) {
	r := &Replica{Name: "dst", LvsName: "disk", LvsUUID: "disk-uuid", Alias: "disk/dst",
		State: types.InstanceStateRunning, IsExposed: true, SpecSize: 2 << 30,
		cloneEntrypointMap: map[string]*CloneEntrypointInfo{}}
	lvols := map[string]*spdktypes.BdevInfo{}
	add := func(name, parent string, snapshot bool, size uint64) *Lvol {
		bdev := &spdktypes.BdevInfo{
			BdevInfoBasic: spdktypes.BdevInfoBasic{UUID: "uuid-" + name, Aliases: []string{"disk/" + name}, BlockSize: 512, NumBlocks: size / 512},
			DriverSpecific: &spdktypes.BdevDriverSpecific{Lvol: &spdktypes.BdevDriverSpecificLvol{
				LvolStoreUUID: r.LvsUUID, BaseSnapshot: parent, Snapshot: snapshot}},
		}
		lvols[name] = bdev
		return &Lvol{Name: name, UUID: bdev.UUID, Alias: bdev.Aliases[0], Parent: parent, SpecSize: size, Children: map[string]*Lvol{}}
	}
	var base *Lvol
	parent := ""
	if clone {
		r.isCloneReplica = true
		r.cloneSourceReplicaName, r.cloneSourceSnapshotName = "src", "base"
		r.cloneEntrypointLvolName = GetCloneEntrypointLvolName("src", "base")
		snap := GetReplicaSnapshotLvolName("src", "base")
		add(snap, "", true, 2<<30)
		base = add(r.cloneEntrypointLvolName, snap, true, 2<<30)
		parent = base.Name
	}
	r.Head = add(r.Name, parent, false, r.SpecSize)
	r.ActiveChain = []*Lvol{base, r.Head}
	if source {
		snap := GetReplicaSnapshotLvolName(r.Name, "child")
		ep := GetCloneEntrypointLvolName(r.Name, "child")
		cached := add(snap, parent, true, 2<<30)
		r.Head.Parent = snap
		lvols[r.Name].DriverSpecific.Lvol.BaseSnapshot = snap
		r.ActiveChain = []*Lvol{base, cached, r.Head}
		add(ep, snap, true, 2<<30)
		add("child", ep, false, 2<<30)
		r.cloneEntrypointMap[ep] = &CloneEntrypointInfo{LvolName: ep, SnapshotName: "child", SnapshotLvolName: snap, CloneReplicas: map[string]bool{"child": true}}
	}
	return r, lvols
}

func TestExpansionStableCloneEligibility(t *testing.T) {
	for _, scenario := range []struct {
		name          string
		clone, source bool
	}{{"ordinary", false, false}, {"clone", true, false}, {"source", false, true}, {"nested-source", true, true}} {
		t.Run(scenario.name, func(t *testing.T) {
			r, lvols := expansionCloneFixture(scenario.clone, scenario.source)
			before, err := json.Marshal(lvols)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.validateExpansionEligibilityNoLock(); err != nil {
				t.Fatal(err)
			}
			if err := r.validateExpansionTopologyNoLock(lvols); err != nil {
				t.Fatal(err)
			}
			after, err := json.Marshal(lvols)
			if err != nil || string(before) != string(after) {
				t.Fatal("eligibility mutated actual topology")
			}
			// Physical partial growth is valid with an old advertised size; only
			// the head changes. Its immutable parents retain their old capacity.
			lvols[r.Name].NumBlocks = (3 << 30) / 512
			if err := r.validateExpansionTopologyNoLock(lvols); err != nil {
				t.Fatal(err)
			}
			if r.SpecSize != 2<<30 || r.Head.SpecSize != 2<<30 || r.isCloneReplica != scenario.clone {
				t.Fatal("precheck changed advertised size or clone identity")
			}
		})
	}
}

func TestExpansionCloneBusyOperationsRejected(t *testing.T) {
	for _, scenario := range []struct {
		name string
		busy func(*Replica)
		want string
	}{
		{"not-running", func(r *Replica) { r.State = types.InstanceStateError }, "not running"},
		{"rebuild-destination", func(r *Replica) { r.isRebuilding = true }, "rebuild"},
		{"rebuild-source", func(r *Replica) { r.rebuildingSrcCache.dstReplicaName = "other" }, "rebuild"},
		{"restore", func(r *Replica) { r.isRestoring = true }, "restore"},
		{"clone-destination", func(r *Replica) { r.isSnapshotCloning = true }, "clone operation"},
		{"clone-source", func(r *Replica) { r.snapshotCloningSrcCache = map[string]*SnapshotCloningSrcCache{"other": {}} }, "clone operation"},
		{"unexported", func(r *Replica) { r.IsExposed = false }, "exported head"},
		{"missing-head", func(r *Replica) { r.Head = nil }, "writable head"},
		{"wrong-chain", func(r *Replica) { r.ActiveChain = r.ActiveChain[:1] }, "active chain"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			r, _ := expansionCloneFixture(true, true)
			scenario.busy(r)
			// Both phases must reject before the nil SPDK client can be used.
			for _, phase := range []string{"replica-precheck", "backend-masked"} {
				server := &Server{replicaMap: map[string]*Replica{r.Name: r}}
				ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(client.ExpansionPhaseMetadataKey, phase))
				_, err := server.ReplicaExpand(ctx, &spdkrpc.ReplicaExpandRequest{Name: r.Name, Size: 3 << 30})
				if err == nil || !strings.Contains(err.Error(), scenario.want) || r.SpecSize != 2<<30 {
					t.Fatalf("phase=%s error=%v size=%d", phase, err, r.SpecSize)
				}
			}
		})
	}
}

func TestExpansionCloneTopologyRejectsBeforeResize(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		change func(*Replica, map[string]*spdktypes.BdevInfo)
	}{
		{"missing-parent", func(r *Replica, m map[string]*spdktypes.BdevInfo) { delete(m, r.cloneEntrypointLvolName) }},
		{"orphan-entrypoint", func(r *Replica, m map[string]*spdktypes.BdevInfo) {
			m[r.cloneEntrypointLvolName].DriverSpecific.Lvol.BaseSnapshot = ""
		}},
		{"wrong-entrypoint", func(r *Replica, _ map[string]*spdktypes.BdevInfo) { r.cloneEntrypointLvolName = "other" }},
		{"missing-source", func(r *Replica, _ map[string]*spdktypes.BdevInfo) { r.cloneSourceReplicaName = "" }},
		{"unrecorded-clone", func(r *Replica, _ map[string]*spdktypes.BdevInfo) { r.isCloneReplica = false }},
		{"nil-chain-base", func(r *Replica, _ map[string]*spdktypes.BdevInfo) { r.ActiveChain[0] = nil }},
		{"cached-parent", func(r *Replica, _ map[string]*spdktypes.BdevInfo) { r.Head.Parent = "other" }},
		{"changed-head-uuid", func(r *Replica, m map[string]*spdktypes.BdevInfo) { m[r.Name].UUID = "replacement" }},
		{"writable-parent", func(r *Replica, m map[string]*spdktypes.BdevInfo) {
			m[r.cloneEntrypointLvolName].DriverSpecific.Lvol.Snapshot = false
		}},
		{"snapshot-head", func(r *Replica, m map[string]*spdktypes.BdevInfo) { m[r.Name].DriverSpecific.Lvol.Snapshot = true }},
		{"non-leaf-head", func(r *Replica, m map[string]*spdktypes.BdevInfo) {
			m[r.Name].DriverSpecific.Lvol.Clones = []string{"other"}
		}},
		{"cycle", func(r *Replica, m map[string]*spdktypes.BdevInfo) {
			m[r.cloneEntrypointLvolName].DriverSpecific.Lvol.BaseSnapshot = r.cloneEntrypointLvolName
		}},
		{"malformed-lvol", func(r *Replica, m map[string]*spdktypes.BdevInfo) { m[r.Name].DriverSpecific = nil }},
		{"source-nil-entry", func(r *Replica, _ map[string]*spdktypes.BdevInfo) {
			for name := range r.cloneEntrypointMap {
				r.cloneEntrypointMap[name] = nil
			}
		}},
		{"source-orphan-entry", func(r *Replica, m map[string]*spdktypes.BdevInfo) {
			for name := range r.cloneEntrypointMap {
				m[name].DriverSpecific.Lvol.BaseSnapshot = ""
			}
		}},
		{"missing-dependent-clone", func(_ *Replica, m map[string]*spdktypes.BdevInfo) { delete(m, "child") }},
		{"wrong-dependent-parent", func(_ *Replica, m map[string]*spdktypes.BdevInfo) { m["child"].DriverSpecific.Lvol.BaseSnapshot = "" }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			r, lvols := expansionCloneFixture(true, true)
			scenario.change(r, lvols)
			if err := r.validateExpansionTopologyNoLock(lvols); err == nil {
				t.Fatal("invalid topology accepted")
			}
			if r.SpecSize != 2<<30 {
				t.Fatal("invalid precheck advanced advertised size")
			}
		})
	}
}

func TestExpansionReplicaExpansionPhaseIsExclusive(t *testing.T) {
	r, _ := expansionCloneFixture(false, false)
	server := &Server{replicaMap: map[string]*Replica{r.Name: r}}
	for _, phases := range [][]string{{"replica-precheck", "backend-masked"}, {"backend-masked", "backend-masked"}, {"invalid"}, {"frontend-suspended"}} {
		pairs := []string{}
		for _, phase := range phases {
			pairs = append(pairs, client.ExpansionPhaseMetadataKey, phase)
		}
		ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(pairs...))
		_, err := server.ReplicaExpand(ctx, &spdkrpc.ReplicaExpandRequest{Name: r.Name, Size: 3 << 30})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("phases=%v error=%v", phases, err)
		}
	}
}

func TestExpansionDisconnectedReplicaConsumerRequirement(t *testing.T) {
	for _, coordinated := range []bool{false, true} {
		for count := range 3 {
			want := 0
			if coordinated {
				want = 1
			}
			if err := validateExpansionConsumerCount(count, coordinated); (err == nil) != (count == want) {
				t.Fatalf("coordinated=%v count=%d error=%v", coordinated, count, err)
			}
		}
	}
}

func TestExpansionDisconnectedRebuildDestinationRetainsDispatch(t *testing.T) {
	r, _ := expansionCloneFixture(false, false)
	r.isRebuilding = true
	r.LvsUUID, r.LvsName = "", ""
	// Missing lvstore identifiers stop this fixture before any RPC. Reaching
	// that check proves rebuild preparation did not enter live busy/lineage checks.
	err := r.Expand(t.Context(), nil, 3<<30)
	if err == nil || !strings.Contains(err.Error(), "either LvsUUID or LvsName") {
		t.Fatalf("disconnected rebuild destination reached wrong workflow: %v", err)
	}
}
