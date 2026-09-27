package drill

// ScenarioID identifies a selectable failure drill.
type ScenarioID string

const (
	// ScenarioMajorityUnreachable: a majority of voters becomes unreachable
	// from the serving leader; new writes must not be confirmed.
	ScenarioMajorityUnreachable ScenarioID = "majority-unreachable"
	// ScenarioLeaderCrashDedup: leader crashes while a write outcome is
	// unknown; retrying the same request must apply it at most once.
	ScenarioLeaderCrashDedup ScenarioID = "leader-crash-dedup"
	// ScenarioSnapshotCatchup: a node stays offline across a compaction
	// boundary, then catches up through InstallSnapshot.
	ScenarioSnapshotCatchup ScenarioID = "snapshot-catchup"
	// ScenarioMembershipLeaderSwitch: a membership change is in progress when
	// the leader switches; the change converges without a half-update.
	ScenarioMembershipLeaderSwitch ScenarioID = "membership-leader-switch"
)

// AllScenarios is the fixed execution order used by "run all".
func AllScenarios() []ScenarioID {
	return []ScenarioID{
		ScenarioMajorityUnreachable,
		ScenarioLeaderCrashDedup,
		ScenarioSnapshotCatchup,
		ScenarioMembershipLeaderSwitch,
	}
}

// Options selects drills and controls failure-injection sabotage used by the
// negative (fault-did-not-take-effect) automated cases.
type Options struct {
	// Scenarios to execute; nil/empty means AllScenarios() in fixed order.
	Scenarios []ScenarioID
	// Sabotage injects a "fault" that does not actually take effect. A valid
	// drill must then FAIL with evidence that the claimed condition was not
	// met. Only honored by tests.
	Sabotage map[ScenarioID]bool
}
