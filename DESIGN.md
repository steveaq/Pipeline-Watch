# Pipeline Watch interface notes

The dark palette and monospace navigation draw on Bergside/TypeUI's
[Mono skill](https://github.com/bergside/awesome-design-skills/blob/main/skills/mono/SKILL.md).
The interface has six routed pages, with the current warehouse shared across them.
The overview starts with publication counts and operational evidence.

The copy was edited using the local Humanizer skill. Say what the pipeline does,
what a count includes, and what needs checking. Remove slogans and fictional
production claims. Do not call a snapshot representative or describe a successful
publication as proof that every record is correct.

## Visual system

Background #10191c, surface #142125, text #e4efea, muted #a9bcb6,
borders #2c4142, mint #9fe8c8, amber #e8bd79, red #ed9595 and blue #89b9d6.
Use system sans for reading and monospace for SQL and the application name.
Titles are 38px, captions 12px, and buttons at least 44px high.

Colour supplements status text. Red identifies failed attempts; amber indicates
quarantine or drift. Neither should imply that the current publication disappeared.
Plots are interactive, with tables or downloads available for detailed inspection.

## Page boundaries

Overview counts current publications. Runs and replay includes every attempt and
its processing time. Quality separates overlapping rule failures from rejected-row
counts. Contracts and lineage shows field requirements, drift and source hashes.
Warehouse analytics uses accepted trips and discloses coverage. SQL and dbt contains
the restricted query runner, physical plans, model SQL and build results.

## Check before release

- Switch pages without losing the current session warehouse.
- Replay a month and confirm that publication counts stay stable.
- Run a schema failure and check both the error and retained publication.
- Check profiling caps, percentile trimming and date-coverage captions.
- Confirm queries and EXPLAIN reject writes and external access.
- Inspect desktop/narrow navigation, contrast, focus and reduced-motion support.
