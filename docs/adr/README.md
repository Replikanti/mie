# Architecture Decision Records

ADR-001 … ADR-024 are the accepted baseline of the design document set and
live together in [`../design/BTC_MIE_ADRs.md`](../design/BTC_MIE_ADRs.md).

Decisions made during implementation get one file each here, continuing the
numbering:

- **File**: `NNN-kebab-slug.md`, where `NNN` is the next free number. Numbers
  are never reused; rejected ADRs keep their file.
- **Title line**: `# ADR-NNN: <title>`.
- **Header**: `- Status: proposed | accepted | rejected | superseded by ADR-MMM`
  and `- Date: YYYY-MM-DD`. An accepted ADR that received a justification
  addendum adds `- Amended: YYYY-MM-DD (#N)` after `- Date:`, one line per
  addendum.
- **Sections**: `Context`, `Decision`, `Consequences` (including the negative
  ones), `Alternatives considered`. A proposed ADR adds `Accept when` — the
  event or measurement that settles it.
- **Lifecycle**: a proposed ADR may be edited freely. An accepted ADR changes
  only its status line, apart from a justification addendum (below); changing
  the decision means a new ADR that supersedes it. The same applies to the
  baseline ADR-001 … ADR-024.
- **Justification addendum**: an accepted ADR in this directory (not the
  baseline file) may receive a dated
  `## Justification addendum (YYYY-MM-DD)` section, appended at the end of
  the file, plus the `- Amended:` header line, where `#N` is the issue that
  asked for it. The addendum only explains why the existing decisions and
  numbers hold: their source, their derivation, what breaks at a different
  value, and the true history of when each value was set. It changes no
  value, threshold, acceptance criterion or normative statement. If
  justifying a number shows that the number is wrong, the new value goes
  through a superseding ADR. Review gate: the diff of the original sections
  is empty, except for the `- Amended:` line and the appended addendum. Why
  this exists: superseding a decision that still stands would mark it as
  replaced, and rewriting accepted text would erase what was decided on
  which evidence.
- **When**: a PR that makes a design decision adds or flips an ADR, and issue
  plans name the ADRs they touch.

Index: `ls docs/adr/`. Status index: `grep -H '^- Status' docs/adr/[0-9]*.md`.
Addendum index: `grep -H '^- Amended' docs/adr/[0-9]*.md`.
