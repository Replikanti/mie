# Architecture Decision Records

ADR-001 … ADR-024 are the accepted baseline of the design document set and
live together in [`../design/BTC_MIE_ADRs.md`](../design/BTC_MIE_ADRs.md).

Decisions made during implementation get one file each here, continuing the
numbering:

- **File**: `NNN-kebab-slug.md`, where `NNN` is the next free number. Numbers
  are never reused; rejected ADRs keep their file.
- **Title line**: `# ADR-NNN: <title>`.
- **Header**: `- Status: proposed | accepted | rejected | superseded by ADR-MMM`
  and `- Date: YYYY-MM-DD`.
- **Sections**: `Context`, `Decision`, `Consequences` (including the negative
  ones), `Alternatives considered`. A proposed ADR adds `Accept when` — the
  event or measurement that settles it.
- **Lifecycle**: a proposed ADR may be edited freely. An accepted ADR changes
  only its status line; changing the decision means a new ADR that supersedes
  it. The same applies to the baseline ADR-001 … ADR-024.
- **Justification addendum**: an accepted ADR may receive a dated
  **`Justification addendum (YYYY-MM-DD)`** section, plus a header line
  `- Amended: YYYY-MM-DD (#N)`.
  - The addendum may only explain why the existing decision and numbers
    hold: their source, derivation, what breaks at a different value, and
    the true history of when each value was set.
  - It must not change any value, threshold, acceptance criterion or
    normative statement. If justifying a number shows the number is wrong,
    the new value goes through a superseding ADR instead.
  - QA/review gate: the diff of the original ADR sections is empty, except
    for the header line and the appended addendum.
- **When**: a PR that makes a design decision adds or flips an ADR, and issue
  plans name the ADRs they touch.

Index: `ls docs/adr/`. Status index: `grep -H '^- Status' docs/adr/[0-9]*.md`.
