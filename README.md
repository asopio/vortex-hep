# vortex-hep

Utilities for converting ATLAS ROOT ntuples into compact Vortex datasets and for inspecting the resulting `.vortex` files.

## Roadmap 

- Generic Root file handling framework, allowing systeamtics tree names to be specified via a config file
- Run on streaming XRootD inputs 
- Utilities for reading .vortex files into ROOT RDataFrame
- Benchmark histograms + profile likelihood comparing ROOT / vortex inputs



## What is in this repo

- `root_to_vortex.py` — main converter from ROOT `TTree`s to Vortex
- `inspect_vortex.py` — quick schema and row preview for a `.vortex` file
- `inspect_ggh_sys.py` — ad hoc inspection helper for ggH nominal and photon-systematic ROOT files
- `data_download_slim/mxaod_skim_and_slim_rdf.py` — ROOT RDataFrame skimming/slimming helper for MxAOD-style inputs
- `test_inputs/` — small sample ROOT and Vortex files for local experiments

## Environment

The project uses [pixi](https://pixi.sh/) and currently depends on:

- ROOT
- `vortex-data`
- `uproot`

Create or enter the environment with:

```bash
pixi shell
```

Or run commands directly inside the environment with:

```bash
pixi run python <script>.py ...
```

The pixi environment sets:

- `KRB5CCNAME=$PIXI_PROJECT_ROOT/krb5.cc`

That is relevant if you need Kerberos credentials for remote ROOT/XRootD access.

## Main workflow: ROOT → Vortex

Basic conversion:

```bash
pixi run python root_to_vortex.py INPUT.root -o output.vortex
```

Select a different tree:

```bash
pixi run python root_to_vortex.py INPUT.root -o output.vortex --tree CollectionTree
```

Restrict to specific branches:

```bash
pixi run python root_to_vortex.py INPUT.root -o output.vortex \
  --branches EventInfoAuxDyn.eventNumber EventInfoAuxDyn.runNumber
```

Concatenate multiple ROOT inputs into one Vortex file:

```bash
pixi run python root_to_vortex.py nominal_1.root nominal_2.root -o combined.vortex
```

### Systematic-difference mode

`root_to_vortex.py` can store systematic variations as relative differences to a nominal sample instead of writing full systematic branches.

```bash
pixi run python root_to_vortex.py \
  nominal.root photonsys.root jetsys1.root jetsys2.root \
  -o nominal_multi.vortex \
  --store-sys-diffs
```

In this mode:

- the first input must be the nominal ROOT file
- all remaining inputs are treated as systematic variations
- output columns are named like `<branch>__<systematic_name>__reldiff`
- requested `--branches` must be nominal `HGamEventInfoAuxDyn` branch names

### Matching events across nominal and systematic inputs

If nominal and systematic files do not have identical row ordering or do not contain exactly the same events, use event/run matching:

```bash
pixi run python root_to_vortex.py \
  nominal.root photonsys.root \
  -o matched.vortex \
  --store-sys-diffs \
  --match-event-run-numbers
```

This matches rows using:

- `EventInfoAuxDyn.eventNumber`
- `EventInfoAuxDyn.runNumber`

Missing values are filled with nulls or NaNs as appropriate.

### Quantizing systematic relative differences

Systematic reldiff columns can be quantized to reduce output size:

```bash
pixi run python root_to_vortex.py \
  nominal.root photonsys.root \
  -o nominal_photonsys_float16.vortex \
  --store-sys-diffs \
  --quantize-systs float16
```

Supported values:

- `float32`
- `float16`
- `int16`

### Chunked streaming

Large ROOT inputs are read in chunks with uproot. Adjust the chunk size if needed:

```bash
pixi run python root_to_vortex.py INPUT.root -o output.vortex --chunk-size 200000
```

## Inspecting Vortex output

Preview schema and the first few rows:

```bash
pixi run python inspect_vortex.py output.vortex
```

Show more rows:

```bash
pixi run python inspect_vortex.py output.vortex --rows 20
```

## Sample data

`test_inputs/` contains small ROOT inputs and corresponding `.vortex` outputs, including:

- nominal-only examples
- photon systematic examples
- jet systematic examples
- outputs with no quantization, `float16`, `float32`, and `int16`
- event/run matching examples

These are the easiest way to sanity-check converter behavior locally.

## Skimming/slimming helper

`data_download_slim/mxaod_skim_and_slim_rdf.py` is a separate helper for slimming MxAOD-style ROOT files with ROOT RDataFrame. It appears to be experimental/ad hoc rather than a polished command-line tool, but it is useful as a reference for:

- parsing XSecSkimAndSlim-style config files
- selecting reco/truth branches
- handling systematic containers
- writing reduced ROOT outputs

The same directory also contains example file lists such as `Nominal.txt` and `PhotonSys.txt`.

## Notes and caveats

- `root_to_vortex.py` supports scalar numeric/bool branches; unsupported branch interpretations are skipped.
- `--store-sys-diffs` requires at least one nominal input and one systematic input.
- Streaming event/run matching is implemented only for `--store-sys-diffs` mode.
- Remote file access may require valid Kerberos/XRootD credentials.
- Some helper scripts in this repo are exploratory and may print diagnostic output rather than behaving like stable end-user tools.
