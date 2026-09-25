package report

import (
	"slices"
	"strconv"
	"strings"
)

// lines renders one consumer's block in a compose report: the bucket word, the consumer and what it
// admitted, then why it was not checked, or the lines the reference path renders for one consumer —
// its gates, its clean run's health and notes — followed by the messages nothing judged and how far
// each fault was tried. Its findings render after every block, in one list across consumers.
func (c ConsumerCheck) lines(total int) []string {
	bucket := c.Bucket()
	lines := []string{trim(pad(bucketWord(bucket), statusColumn) + pad(c.Name, consumerColumn) +
		admittedText(c.Admitted, total))}

	switch bucket { //nolint:exhaustive // every checked bucket renders the same way, below.
	case BucketNotSelected, BucketNotCovered, BucketSetup:
		return append(lines, c.reasonLines()...)
	case BucketGate:
		return append(lines, c.Report.violationLines()...)
	}

	checked := c.Report
	lines = append(lines, gatesHeldLine(checked.Gates))
	lines = append(lines, checked.Health.summaryLines()...)
	lines = append(lines, checked.Health.noteLines()...)
	lines = append(lines, checked.notJudgedLines()...)

	return append(lines, checked.coverageLines()...)
}

// reasonLines say why a consumer was not checked, or where its check stopped. A setup error after runs
// completed never claims nothing was replayed.
func (c ConsumerCheck) reasonLines() []string {
	var lines []string

	if c.Reason != "" {
		lines = append(lines, detailIndent+withoutDurations(oneLine(c.Reason)))
	}

	if c.Report.Setup == nil {
		return lines
	}

	for _, line := range setupLines(c.Report.Setup, c.Report.Completed) {
		if line != "" {
			lines = append(lines, detailIndent+withoutDurations(oneLine(line)))
		}
	}

	return lines
}

// notJudgedLines name the messages that produced no effect on the clean run, each with its corpus file:
// no fault aimed at them could show anything, so they count toward no verdict.
func (r Report) notJudgedLines() []string {
	if r.Health == nil || len(r.Health.SilentSeqs) == 0 {
		return nil
	}

	return []string{"", pad(notJudgedLabel, statusColumn) + notJudgedText(Sequences(r.Health.SilentSeqs, r.Files))}
}

// coverageLines are one line per fault the check considered, in the order it tried them: a legal
// fault's pairs as attempted, unexpressed and cut by the budget, or the clause refusing it. Gate mode
// tries no fault, so it renders none.
func (r Report) coverageLines() []string {
	if r.GatesOnly || len(r.Coverage) == 0 {
		return nil
	}

	lines := []string{""}
	legal := 0

	for _, line := range r.Coverage {
		text := illegalCoverage(describeFault(line.Fault), oneLine(line.Clause))
		if line.Legal {
			legal++
			text = legalCoverage(describeFault(line.Fault), line)
		}

		lines = append(lines, detailIndent+coverageLabel+text)
	}

	if legal == 0 {
		lines = append(lines, detailIndent+coverageLabel+noLegalFault)
	}

	return lines
}

// Sequences names message sequences the way a report renders them: each followed by the corpus file it
// came from, wherever one is known.
func Sequences(seqs []uint64, files map[uint64]string) string {
	parts := make([]string, 0, len(seqs))

	for _, seq := range seqs {
		part := "#" + strconv.FormatUint(seq, 10)
		if file, named := files[seq]; named {
			part += " (" + file + ")"
		}

		parts = append(parts, part)
	}

	return strings.Join(parts, ", ")
}

// composeFindingLines renders a finding on the compose path, whose notes advise only what a compose
// user can do.
func composeFindingLines(f Finding) []string {
	reservations := slices.Clone(f.Reservations)
	for index, note := range reservations {
		reservations[index] = composeNote(note)
	}

	f.Reservations = reservations

	return findingLines(f)
}

// composeGuardDependentLines is the guard-dependence note on the compose path.
func composeGuardDependentLines(findings []Finding) []string {
	return guardDependentNote(findings, composeGuardNextStep)
}

// composeNote maps a reference note advising what only a Go caller can do to the compose path's own.
// Every other note is kept as it is.
func composeNote(note string) string {
	switch note {
	case mailDivergenceNote:
		return composeMailNote
	case unjudgeableEgressNote:
		return composeEgressNote
	case readDivergenceNote:
		return composeReadNote
	case unclassifiedNote:
		return composeUnclassifiedNote
	case guardOverrideNote:
		return composeGuardOverrideNote
	default:
		return note
	}
}
