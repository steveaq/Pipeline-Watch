# Pipeline Watch design system

## Intent

An operations console in which every state has evidence. Apply the MIT-licensed
[Mono skill](https://github.com/bergside/awesome-design-skills/blob/main/skills/mono/SKILL.md)
from TypeUI/Bergside's Awesome Design Skills collection. Adapt its dense, high
contrast foundations into a restrained dark console rather than a decorative terminal.

## Foundations

Background #10191c; surface #142125; raised #19292c; ink #e4efea;
muted #9db2ae; borders #2c4142. Mint #9fe8c8 indicates published data/actions;
amber #e8bd79 indicates quarantine/warnings; red #ed9595 indicates failed attempts;
blue #89b9d6 indicates source input. Use system sans for reading and system mono
for labels, identifiers and SQL. No external fonts. 8px spacing rhythm, 44px minimum
targets, 3–5px corners, no decorative animation.

## Components

Masthead identifies the warehouse context. The hero's terminal reports actual
partition/run state. The control plane names input mode and month before execution.
Stage cards expose source, quarantine, silver and gold counts. Monitor, quality,
lineage and SQL tabs reveal evidence behind the summary. Status always has text;
colour supplements it. Counts come from DuckDB, never hard-coded demo dashboards.

Buttons have hover, keyboard focus, loading and error feedback. A completed run
names published, replaced and quarantined rows. Failed schema drills explicitly
preserve published data and remain in the run ledger. Full downloads are explicit;
the default bundled mode never needs the network. SQL errors remain visible.

## Accessibility and responsive behaviour

Maintain AA contrast and visible 3px focus. At 640px the hero, controls and stage
columns stack; labels and SQL remain readable. Native widgets retain labels and
keyboard interactions. Chart labels and the conservation equation communicate
without colour. Support reduced motion and data-table/CSV alternatives.

## Content and scope

Use precise states: success, warning, failed, published, replaced, quarantined.
Distinguish an attempt from a published partition and a snapshot from a full month.
Never label a pipeline 'healthy' solely because the UI loads. Disclose the row cap,
session isolation and temporary host filesystem. Keep implementation detail in
lineage/help, where it explains real operational decisions.

## QA

- Replay a month; silver count must stay stable and the ledger must gain an attempt.
- Drop a required field; publication must survive and the failed run must be visible.
- Inspect raw = valid unique + rejected + duplicate counts.
- Run actual dbt build against the warehouse.
- Verify SQL rejects writes, multiple statements and external file reads.
- Check desktop/narrow layouts, status contrast, focus and long run identifiers.
