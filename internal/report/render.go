package report

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Wintersta7e/stutter/internal/policy"
)

const (
	// statusColumn is the width of the leading verdict column: "FAIL" plus its gap.
	statusColumn = 6
	// consumerColumn is the width of the consumer column.
	consumerColumn = 23
	// faultColumn is the width of the delivery-fault column.
	faultColumn = 25
	// detailIndent prefixes every line that elaborates on the verdict line above it.
	detailIndent = "      "
	// cleanLabel and mutatedLabel are padded to the same width so the two outcomes line up under
	// each other, which is the whole point of printing them together.
	cleanLabel   = "clean run:   "
	mutatedLabel = "mutated run: "
	// thousandsGroup is how many digits sit between separators in a rendered count.
	thousandsGroup = 3
)

// Render writes the report as the text a developer reads.
//
// It is deliberately separate from ExitCode: the verdict must be computable without producing text,
// so that a caller can decide first and print second, and so that the decision can be tested on its
// own.
func (r Report) Render(w io.Writer) error {
	text := strings.Join(r.lines(), "\n") + "\n"

	if _, err := io.WriteString(w, text); err != nil {
		return fmt.Errorf("write report: %w", err)
	}

	return nil
}

// String renders the report as a string, for callers that are not writing to a stream.
func (r Report) String() string {
	return strings.Join(r.lines(), "\n") + "\n"
}

func (r Report) lines() []string {
	if r.Setup != nil {
		return setupLines(r.Setup, r.Completed)
	}

	lines := []string{scanLine(r.Scan)}

	if !gatesHeld(r.Gates) {
		return append(lines, r.violationLines()...)
	}

	lines = append(lines, gatesHeldLine(r.Gates))
	lines = append(lines, r.Health.summaryLines()...)

	for _, finding := range r.Findings {
		lines = append(lines, "")
		lines = append(lines, findingLines(finding)...)
	}

	if note := guardDependentLines(r.Findings); note != nil {
		lines = append(lines, "")
		lines = append(lines, note...)
	}

	// Beside the PASS line, because a clean run that refused messages or did nothing with some of
	// them is above all a reason to doubt a pass.
	lines = append(lines, r.Health.noteLines()...)

	if line, shown := r.passLine(); shown {
		lines = append(lines, "", line)
	}

	return lines
}

// guardDependentLines separates "Stutter could not tell" from "this is probably fine".
//
// Both render as WARN, and without this they read identically — so the one that has a next step
// gets ignored alongside the one that does not.
func guardDependentLines(findings []Finding) []string {
	return guardDependentNote(findings,
		"divergence was acceptable. Override the stub named above to get a firm verdict.")
}

// guardDependentNote is the guard-dependence note, closing with the next step its reader can take.
func guardDependentNote(findings []Finding, nextStep string) []string {
	warnings, guarded := 0, 0

	for _, finding := range findings {
		if finding.Status != StatusWarn {
			continue
		}

		warnings++

		if finding.Confidence == ConfidenceGuardDependent {
			guarded++
		}
	}

	if guarded == 0 {
		return nil
	}

	verb := " are "
	if guarded == 1 {
		verb = " is "
	}

	// "1 of 1 warning" reads as a rounding error rather than as a count.
	share := plural(guarded, "warning")
	if guarded != warnings {
		share = strconv.Itoa(guarded) + " of " + plural(warnings, "warning")
	}

	return []string{
		pad("NOTE", statusColumn) + share + verb +
			"guard-dependent: Stutter could not decide, rather than deciding the",
		detailIndent + nextStep,
	}
}

// setupLines renders a check that could not be carried out. A setup error is not evidence about the
// consumers under test, so no verdict follows it; before any run completed nothing was replayed, and
// saying exactly that is the whole message, but after runs completed that sentence would be false.
func setupLines(err error, completed int) []string {
	closing := "Nothing was replayed, so no gate ran and no findings were computed."
	if completed > 0 {
		closing = setupAfterRuns(completed)
	}

	return []string{"Setup failed: " + err.Error(), "", closing}
}

func scanLine(scan Scan) string {
	return "Scanned " + plural(scan.Consumers, "consumer") +
		" over " + plural(scan.Messages, "recorded message") + "."
}

// gatesHeldLine names the gates that ran, so a green report proves the gates executed rather than
// leaving their silence to be read as success.
func gatesHeldLine(gates []GateCheck) string {
	if len(gates) == 0 {
		return "Gates: none were run, so nothing vouches for the comparisons below."
	}

	names := make([]string, len(gates))
	for index, check := range gates {
		names[index] = string(check.Name)
	}

	return "Gates held: " + strings.Join(names, ", ") + "."
}

// violationLines renders a violated gate.
//
// A gate violation is never rendered as a list of findings with a caveat: downstream of a violated
// gate nothing was computed, and a caveated list would still put noise in front of a user.
func (r Report) violationLines() []string {
	var lines []string

	for _, check := range r.Violations() {
		lines = append(lines, "",
			trim(pad("GATE", statusColumn)+pad(string(check.Name), consumerColumn)+"violated"))
		lines = append(lines, indent(check.describe(), detailIndent)...)
	}

	// The clean run's health is the diagnosis: it is what says where to look next.
	if summary := r.Health.summaryLines(); summary != nil {
		lines = append(lines, "")
		lines = append(lines, summary...)
	}

	lines = append(lines, r.Health.noteLines()...)

	return append(lines, "",
		"No findings were computed. Every comparison downstream of a violated gate is noise, so the",
		"report is withheld rather than qualified. This is not a test failure.")
}

// describe explains a violated gate, naming the fault that exposed it when one did.
func (c GateCheck) describe() string {
	if c.Fault == policy.FaultNone {
		return c.Result.Describe()
	}

	return describeFault(c.Fault) + ": " + c.Result.Describe()
}

// summaryLines states what the clean run saw, so a verdict shows the evidence it rests on. When the
// run saw nothing, it also says where to look: which of connecting, consuming and writing never
// happened is the difference between a wrong address and a missing proxy.
func (h *Health) summaryLines() []string {
	if h == nil {
		return nil
	}

	summary := "Clean run: " + plural(h.Messages, "message") + " delivered " + plural(h.Delivered, "time") +
		", producing " + plural(h.Effects, "effect") + "."

	if h.Effects > 0 {
		return []string{summary}
	}

	switch {
	case h.Delivered > 0:
		return []string{
			summary,
			detailIndent + "The service took delivery and no proxy saw it do anything: check that every",
			detailIndent + "dependency it writes to is reached through the address Stutter handed it.",
		}
	case h.Setup > 0:
		return []string{
			summary,
			detailIndent + plural(h.Setup, "effect") + " before the first delivery show the service connected,",
			detailIndent + "but it never took delivery of a message: check the stream and subject it consumes.",
		}
	default:
		return []string{
			summary,
			detailIndent + "Nothing reached any proxy: the service may never have connected, or may be",
			detailIndent + "dialling its dependencies directly instead of through the addresses Stutter handed it.",
		}
	}
}

// noteLines names what in the clean run undermines a verdict built on it. Each is observed signal:
// none changes the exit code, and none is rendered when there is nothing to say.
func (h *Health) noteLines() []string {
	if h == nil {
		return nil
	}

	var lines []string

	if h.Failed > 0 {
		lines = append(lines, "",
			pad("NOTE", statusColumn)+strconv.Itoa(h.Failed)+" of "+plural(h.Delivered, "delivery attempt")+
				" failed on the clean run: a message the service refuses",
			detailIndent+"does no work, so a fault aimed at it has nothing to repeat.")
	}

	// With no effects at all the observation gate has already said this, and louder.
	if h.Silent > 0 && h.Effects > 0 {
		lines = append(lines, "",
			pad("NOTE", statusColumn)+strconv.Itoa(h.Silent)+" of "+plural(h.Messages, "message")+
				" produced no effect on the clean run: a fault aimed there",
			detailIndent+"had nothing to repeat.")
	}

	if h.Late > 0 {
		lines = append(lines, "",
			pad("NOTE", statusColumn)+plural(h.Late, "late effect")+" on the clean run: work that finished after"+
				" its message's window",
			detailIndent+"closed. Work a handler does not wait for is the likeliest source of instability.")
	}

	lines = append(lines, h.endNoteLines()...)

	return append(lines, h.busNoteLines()...)
}

// endNoteLines names how the clean run ended where that qualifies it: messages still owed when it was
// cut, messages that reached the delivery cap, a service that exited by itself. Each renders only when
// there is something to say.
func (h *Health) endNoteLines() []string {
	var lines []string

	if h.Owed > 0 {
		lines = append(lines, "",
			pad("NOTE", statusColumn)+strconv.Itoa(h.Owed)+" of "+plural(h.Messages, "message")+
				" were still owed an acknowledgement when the bus fell silent:",
			detailIndent+"the run was cut there, the same way in every run.")
	}

	if h.Exhausted > 0 {
		lines = append(lines, "",
			pad("NOTE", statusColumn)+plural(h.Exhausted, "message")+" reached the cap of "+
				strconv.Itoa(h.DeliveryCap)+" deliveries without an acknowledgement;",
			detailIndent+"the consumer allows more, which were not observed.")
	}

	if h.Exit.Exited {
		lines = append(lines, "",
			pad("NOTE", statusColumn)+"the service exited on its own after message #"+
				strconv.FormatUint(h.Exit.After, 10)+": "+h.Exit.Describe())
	}

	return lines
}

// busNoteLines names what the bus refused and counted on the clean run. Each renders only when there
// is something to say, so a run that saw none of it renders as it always did.
func (h *Health) busNoteLines() []string {
	var lines []string

	if len(h.Refusals) > 0 {
		lines = append(lines, "",
			pad("NOTE", statusColumn)+"the bus refused "+plural(len(h.Refusals), "request")+
				" before the first delivery; a service whose startup",
			detailIndent+"is refused may never consume:")

		for _, refusal := range h.Refusals {
			lines = append(lines, detailIndent+oneLine(refusal.Subject+" "+strconv.Itoa(refusal.ErrCode)+" "+
				refusal.Description))
		}
	}

	lines = appendCount(lines, h.NoResponders, "request", " found no responder on the clean run:",
		"nothing was subscribed to answer them. Each still counts as the service's work.")
	lines = appendCount(lines, h.FedBack, "message", " Stutter did not publish reached the consumer and did nothing:",
		"its own output, fed back through the stream it consumes, left out of the comparison.")
	lines = appendCount(lines, h.Elsewhere, "message", " on another stream arrived inside a message's window:",
		"the service's own bus work, neither scoped nor checked.")

	return appendCount(lines, h.ClosedAfterInfo, "bus client", " hung up after the greeting without sending a byte:",
		"likely a client that requires TLS, or a script waiting for the port to open.")
}

// appendCount adds one NOTE for a non-zero count: the count and what it is, then what it means.
func appendCount(lines []string, count int, noun, what, meaning string) []string {
	if count == 0 {
		return lines
	}

	return append(lines, "", pad("NOTE", statusColumn)+plural(count, noun)+what, detailIndent+meaning)
}

// oneLine collapses every run of whitespace to one space: one value renders on one line.
func oneLine(text string) string {
	return strings.Join(strings.Fields(text), " ")
}

// findingLines renders one finding: the verdict line, then everything needed to act on it without
// reading Stutter's source.
func findingLines(f Finding) []string {
	verdict := pad(string(f.Status), statusColumn) +
		pad(f.Consumer, consumerColumn) +
		pad(describeFault(f.Fault), faultColumn) + f.Summary

	// The qualifier rides on the verdict line rather than only on a reservation below it, so a
	// finding Stutter could not decide is distinguishable while skimming the left-hand column.
	if f.Confidence != ConfidenceFirm {
		verdict = trim(verdict) + "  [" + string(f.Confidence) + "]"
	}

	lines := []string{trim(verdict)}

	lines = appendDetail(lines, "minimal repro: ", f.Repro)
	lines = appendDetail(lines, cleanLabel, f.Clean)
	lines = appendDetail(lines, mutatedLabel, f.Mutated)
	lines = appendDetail(lines, "legal because: ", f.Clause)

	for _, note := range f.Notes {
		lines = append(lines, detailIndent+note)
	}

	for _, note := range f.Reservations {
		lines = append(lines, detailIndent+note)
	}

	return lines
}

func appendDetail(lines []string, label, value string) []string {
	if value == "" {
		return lines
	}

	return append(lines, detailIndent+label+value)
}

// passLine closes the report with the consumers nothing was found against, and reports whether it
// is worth printing at all.
//
// Gate mode injects nothing, so nothing it did can have been survived: its closing line says the gates
// held and never says PASS.
func (r Report) passLine() (string, bool) {
	if r.GatesOnly {
		return pad(string(StatusHeld), statusColumn) + plural(r.Scan.Consumers, "consumer") +
			": the gates held; gate mode injects no faults, so this is not a pass", true
	}

	passed := max(r.Scan.Consumers-flaggedConsumers(r.Findings), 0)
	if passed == 0 && r.Silenced == 0 {
		return "", false
	}

	line := pad("PASS", statusColumn)

	if len(r.Findings) > 0 {
		line += plural(passed, "other consumer")
	} else {
		line += plural(passed, "consumer")
	}

	if r.Silenced > 0 {
		line += " (" + plural(r.Silenced, "divergence") + " silenced by a declared invariant)"
	}

	return line, true
}

// flaggedConsumers counts the distinct consumers carrying a finding, so one consumer with three
// findings is not subtracted three times from the pass count.
func flaggedConsumers(findings []Finding) int {
	seen := make(map[string]struct{}, len(findings))
	for _, finding := range findings {
		seen[finding.Consumer] = struct{}{}
	}

	return len(seen)
}

// describeFault renders a fault the way a user would say it out loud.
func describeFault(fault policy.Fault) string {
	switch fault {
	case policy.FaultNone:
		return "no fault"
	case policy.FaultDuplicate:
		return "duplicate delivery"
	case policy.FaultCrashBeforeAck:
		return "crash before ack"
	case policy.FaultReorder:
		return "reordering"
	case policy.FaultDelay:
		return "delayed delivery"
	case policy.FaultConcurrent:
		return "concurrent delivery"
	case policy.FaultDrop:
		return "dropped delivery"
	default:
		return string(fault)
	}
}

// indent prefixes every line of a multi-line block, preserving the relative indentation inside it
// so a quoted effect stays aligned under the sentence that introduced it.
func indent(text, prefix string) []string {
	lines := strings.Split(text, "\n")
	for index, line := range lines {
		if line == "" {
			continue
		}

		lines[index] = prefix + line
	}

	return lines
}

// pad left-aligns a value in a fixed column, keeping one space of separation when it overflows
// rather than running two fields together.
func pad(value string, width int) string {
	length := utf8.RuneCountInString(value)
	if length+1 > width {
		return value + " "
	}

	return value + strings.Repeat(" ", width-length)
}

// trim removes the padding of an empty trailing column, so no line carries invisible whitespace.
func trim(line string) string {
	return strings.TrimRight(line, " ")
}

// plural renders a count with group separators and the matching form of its noun.
func plural(count int, noun string) string {
	if count == 1 {
		return thousands(count) + " " + noun
	}

	return thousands(count) + " " + noun + "s"
}

// thousands renders an integer with group separators, so a corpus size is readable at a glance.
func thousands(value int) string {
	digits := strconv.Itoa(value)

	sign := ""
	if strings.HasPrefix(digits, "-") {
		sign, digits = "-", digits[1:]
	}

	head := len(digits) % thousandsGroup

	groups := make([]string, 0, len(digits)/thousandsGroup+1)
	if head > 0 {
		groups = append(groups, digits[:head])
	}

	for index := head; index < len(digits); index += thousandsGroup {
		groups = append(groups, digits[index:index+thousandsGroup])
	}

	return sign + strings.Join(groups, ",")
}
