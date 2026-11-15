# Node 3 processing files

Place these directories at the repository root:

- `indexing/build_index.py` — distributed inverted-index builder
- `pagerank/pagerank.py` — PageRank graph computation
- `export/export.py` — validated Parquet serving export
- `scripts/node3_run.sh` — runs the three stages in order

The runner expects the Iceberg catalog and Spark environment to be configured
on Node 3. It uses the project cluster defaults for HDFS and Spark Master.

IMPORTANT:
- These are the current Node 3 scripts retrieved from the repository's `main`
  branch, arranged into the directory layout expected by the runner.
- The source repository currently also has root-level copies of the three
  Python scripts and `node3_run.sh`; the runner's relative paths expect the
  nested layout shown here. Do not overwrite or execute production outputs
  blindly. First compare against the current working tree on Node 3.
- The runner's default target is the production table `lake.search.documents`
  and export base `/export`. Do not run `phase0` or `phase1` against production
  until the team has explicitly authorized it. For a safe rehearsal, supply
  a throwaway table and export base, for example:
  `bash scripts/node3_run.sh rehearsal1 lake.search.<rehearsal_table> /tmp/rehearsal/export`
