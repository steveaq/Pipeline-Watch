# Pipeline Watch

![Application preview](docs/preview.png)

A working NYC taxi batch pipeline with a Streamlit observability console. Follow
source files through validation, quarantine, partition publication and a dbt gold
model. The run ledger and quality metrics are read from the real DuckDB warehouse.

## Run

Python 3.11 recommended.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m streamlit run app.py
```

No keys are required. On first visit, the app creates an isolated warehouse for
the browser session using three committed January–March 2025 green-taxi snapshots.
Each contains the first 5,000 rows in original source order. The interface labels
this row-capped mode; it is **not a representative statistical sample**.
Provenance, source URLs, retrieval time, full-source row counts and both source and
snapshot checksums are recorded in `data/bundled/manifest.json`.

## Exercise the pipeline

- **Run partition:** replay a bundled month or fetch the entire official TLC month.
- **Run monitor:** inspect row conservation, publication counts and every attempt.
- **Quality desk:** inspect check failures, source-row payloads and rejection reasons.
- **Schema-failure drill:** drop a required field from a temporary copy; confirm
  the attempt is recorded as failed and published data remains intact.
- **Lineage & dbt:** inspect source checksums/drift and run a real `dbt build`.
- **SQL workbench:** query the warehouse with one read-only SELECT; external
  file/network access is disabled and output is capped at 500 rows.

## CLI

```bash
python pipeline.py demo
python pipeline.py run --month 2025-03 --mode live
python pipeline.py dbt
```

The CLI defaults to `runtime/warehouse.duckdb`. Use `--database /absolute/path/db.duckdb`
for a durable local warehouse. `demo --mode live` downloads all three complete months.
`runtime/`, downloaded source files and dbt build artifacts are ignored by Git.

## Architecture

1. **Bronze:** immutable Parquet copies addressed by SHA-256; original input schemas
   and checksums are recorded for audit, including failed inputs.
2. **Validation:** required columns, timestamps, positive duration ≤24h, partition
   month, distance >0 and ≤250 miles, finite nonnegative fare/total, valid zone IDs.
   These bounds are explicit demonstration policies, not TLC error certifications.
3. **Silver:** canonical core-field row hashes, in-batch deduplication, typed trips.
   A successful run atomically replaces one partition; reruns never append duplicates.
4. **Gold:** zone-by-day trips, revenue, distance and duration. `zone_daily.sql` is
   used both by the Python publication and dbt, so there is one transformation definition.
5. **Audit:** run timestamps, mode, schema drift, row counts, rejects and errors.
   Raw rows = accepted unique rows + rejected rows + valid duplicate copies.

Missing required schemas or zero valid rows fail without replacing the previous
partition. Other schema changes are recorded. Quality failures can overlap; the
quarantine counts each rejected source row once. The ledger retains all attempts;
dashboard conservation counts describe only the currently published partitions.

## Verify

```bash
python -m unittest test_pipeline.py
python pipeline.py demo
python pipeline.py dbt
```

Regression tests cover replay, row conservation, quarantine, schema failure,
partition replacement/drift, all-invalid input, query restrictions and Streamlit.
GitHub Actions also builds the sample warehouse and runs dbt's schema/SQL tests.

## Source and hosting

[NYC TLC official trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
and its taxi-zone lookup. Public records are vendor supplied and can contain
incomplete or inaccurate values. Data keeps upstream provenance and terms.

For Streamlit Community Cloud, use this repository, branch `main`, entry point
`app.py`, Python 3.11. This is a portfolio-sized single-process app: browser sessions
get separate warehouses, and host filesystem state may be temporary. Use the CLI
and a persistent disk for durable ingestion. It has no production scheduler,
authentication or shared multi-writer warehouse. The optional paid Render
configuration does not provision or charge anything until explicitly deployed.

See `DESIGN.md` for the visual system. Application code is MIT-licensed.
