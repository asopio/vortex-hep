# AGENTS.md

## Repo purpose

This repository is a small HEP utility project for converting ATLAS H→γγ ROOT ntuples into Vortex datasets and inspecting the results.

Primary script:

- `root_to_vortex.py`

Secondary helpers:

- `inspect_vortex.py`
- `inspect_ggh_sys.py`
- `data_download_slim/mxaod_skim_and_slim_rdf.py`

## Environment and commands

- Use `pixi run ...` for project commands.
- Python is the main runtime in this repo.
- Dependencies are defined in `pixi.toml`; current key ones are ROOT, `vortex-data`, and `uproot`.
- The pixi environment sets `KRB5CCNAME` to `krb5.cc` in the project root.

Common commands:

```bash
pixi run python root_to_vortex.py INPUT.root -o output.vortex
pixi run python inspect_vortex.py output.vortex
```

## Important behavior in `root_to_vortex.py`

`root_to_vortex.py` supports two main modes:

1. Plain conversion of one or more ROOT files into a Vortex dataset.
2. `--store-sys-diffs` mode, where the first input is nominal and later inputs are systematic variations stored as relative differences.

Important flags:

- `--tree`
- `--branches`
- `--store-sys-diffs`
- `--chunk-size`
- `--match-event-run-numbers`
- `--quantize-systs {float32,float16,int16}`

Key constraints:

- `--store-sys-diffs` requires at least two inputs.
- `--match-event-run-numbers` is only implemented for `--store-sys-diffs` streaming mode.
- In matching mode, rows are aligned with `EventInfoAuxDyn.eventNumber` and `EventInfoAuxDyn.runNumber`.
- Requested branches in sys-diff mode must be nominal `HGamEventInfoAuxDyn` branches.
- Output reldiff columns are named `<branch>__<systematic_name>__reldiff`.
- The converter is written around scalar numeric/bool branches; do not assume jagged/object branches are supported.

## Data and fixtures

- `test_inputs/` contains small local ROOT fixtures and expected/example `.vortex` outputs.
- Use those files for local testing before trying remote production inputs.
- `data_download_slim/Nominal.txt` and `data_download_slim/PhotonSys.txt` are remote ROOT file lists.

## Guidance for edits

- Prefer minimal, targeted changes; this repo is mostly utility scripts.
- Preserve CLI flag names and output column naming exactly.
- Do not introduce refactors unless the task requires them.
- If changing conversion logic, validate against files in `test_inputs/`.
- If using shell commands for verification, keep them read-only unless the user explicitly requests otherwise.

## Caveats

- `inspect_ggh_sys.py` is an exploratory inspection script, not a general interface.
- `data_download_slim/mxaod_skim_and_slim_rdf.py` contains signs of in-progress/debug behavior and should be treated cautiously.
- Remote ROOT/XRootD access may depend on external credentials not stored in the repo.
