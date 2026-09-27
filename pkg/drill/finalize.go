package drill

import "time"

// finish converges the cluster, builds the ordered check list and assembles
// the report. Extra checks may assert anything; convergence and a non-empty
// check set are always represented.
func (h *harness) finish(checks []CheckResult, convergeTimeout time.Duration, started time.Time) (*Report, error) {
	converged, _, digest, final := h.converge(convergeTimeout)
	if !converged {
		checks = append(checks, CheckResult{
			Name:   "final-convergence",
			Pass:   false,
			Detail: "online nodes did not reach equal commit/applied and history digest",
		})
	} else {
		checks = append(checks, CheckResult{
			Name:   "final-convergence",
			Pass:   true,
			Detail: "all online nodes share commit, applied and history digest",
		})
	}
	return h.buildReport(checks, final, digest, converged, started)
}

func checkPass(name, detail string) CheckResult {
	return CheckResult{Name: name, Pass: true, Detail: detail}
}

func checkFail(name, detail string) CheckResult {
	return CheckResult{Name: name, Pass: false, Detail: detail}
}
