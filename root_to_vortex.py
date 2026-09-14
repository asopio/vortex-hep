import argparse
import math
import tempfile
import time
from pathlib import Path
from typing import Iterable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as pa_ipc
import uproot
import vortex
import vortex.io

REL_DIFF_EPSILON = 1.0e-12
SUPPORTED_KINDS = {"b", "i", "u", "f"}
NOMINAL_CONTAINER = "HGamEventInfoAuxDyn"
EVENT_NUMBER_BRANCH = "EventInfoAuxDyn.eventNumber"
RUN_NUMBER_BRANCH = "EventInfoAuxDyn.runNumber"
EVENT_RUN_HASH_BRANCH = "EventInfoAuxDyn.eventRunHash"
MATCH_BRANCHES = [EVENT_NUMBER_BRANCH, RUN_NUMBER_BRANCH]
INT16_QUANTIZATION_SCALE = 1 << 13
INT16_MISSING_SENTINEL = -(1 << 15)


class Profiler:
    def __init__(self) -> None:
        self.starts: dict[str, float] = {}

    def start(self, name: str) -> None:
        self.starts[name] = time.perf_counter()
        print(f"[profile] start {name}")

    def stop(self, name: str, **stats: object) -> None:
        started = self.starts.pop(name, None)
        elapsed = 0.0 if started is None else time.perf_counter() - started
        extras = " ".join(f"{key}={value}" for key, value in stats.items())
        suffix = f" {extras}" if extras else ""
        print(f"[profile] done {name} elapsed_s={elapsed:.3f}{suffix}")


PROFILER = Profiler()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Convert ROOT TTrees into a compressed Vortex dataset, optionally "
            "storing systematic branches as relative differences to the nominal sample."
        )
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        help=(
            "Input ROOT files. With --store-sys-diffs, provide NOMINAL_ROOT followed by one or more SYSTEMATIC_ROOT files. "
            "Without it, all inputs are concatenated into one output dataset."
        ),
    )
    parser.add_argument("-o", "--output", required=True, help="Output .vortex file path")
    parser.add_argument("--tree", default="CollectionTree", help="TTree name to read")
    parser.add_argument(
        "--branches",
        nargs="*",
        default=None,
        help=(
            "Optional branch list. For nominal mode use full ROOT branch names. "
            "With --store-sys-diffs use nominal HGamEventInfoAuxDyn branch names."
        ),
    )
    parser.add_argument(
        "--store-sys-diffs",
        action="store_true",
        help=(
            "Store the relative difference (systematic - nominal) / nominal for each systematic variation "
            "instead of storing the complete systematic branches."
        ),
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100_000,
        help="Number of entries per uproot step when streaming data from ROOT.",
    )
    parser.add_argument(
        "--match-event-run-numbers",
        action="store_true",
        help=(
            "Match rows across inputs using EventInfoAuxDyn.eventNumber and EventInfoAuxDyn.runNumber. "
            "Includes events present in only a subset of inputs and fills missing values with null/nan."
        ),
    )
    parser.add_argument(
        "--quantize-systs",
        choices=["float32", "float16", "int16"],
        default=None,
        help="Quantize systematic reldiff columns to the requested storage type.",
    )
    return parser.parse_args()


def open_tree(path: str, tree_name: str):
    root_file = uproot.open(path)
    if tree_name not in root_file:
        raise RuntimeError(f"TTree '{tree_name}' not found in {path}")
    return root_file[tree_name]


def branch_numpy_dtype(branch) -> object | None:
    interpretation = getattr(branch, "interpretation", None)
    numpy_dtype = getattr(interpretation, "numpy_dtype", None)
    if numpy_dtype is None:
        return None
    if getattr(numpy_dtype, "subdtype", None) is not None:
        return None
    if numpy_dtype.kind not in SUPPORTED_KINDS:
        return None
    return numpy_dtype


def filter_supported_branches(tree, requested: list[str] | None) -> list[str]:
    requested_set = set(requested) if requested else None
    available: list[str] = []

    for name, branch in tree.items():
        if requested_set is not None and name not in requested_set:
            continue
        if branch_numpy_dtype(branch) is None:
            continue
        available.append(name)

    if requested_set is not None:
        missing = sorted(requested_set - set(available))
        if missing:
            raise RuntimeError(
                "Requested branches are missing or unsupported: " + ", ".join(missing)
            )
    if not available:
        raise RuntimeError("No supported scalar numeric/bool branches found")
    return available


def numpy_batch_to_record_batch(batch: dict[str, object], branches: list[str]) -> pa.RecordBatch:
    arrays = [pa.array(batch[branch]) for branch in branches]
    return pa.record_batch(arrays, names=branches)


def compute_event_run_hash(event_numbers: pa.Array, run_numbers: pa.Array) -> pa.Array:
    event_int64 = pc.cast(event_numbers, pa.int64())
    run_int64 = pc.cast(run_numbers, pa.int64())
    return pc.bit_wise_or(pc.shift_left(run_int64, 32), event_int64)


def batches_for_tree(tree, branches: list[str], chunk_size: int) -> Iterable[pa.RecordBatch]:
    for batch in tree.iterate(expressions=branches, library="np", step_size=chunk_size):
        yield numpy_batch_to_record_batch(batch, branches)


def table_for_tree(tree, branches: list[str], chunk_size: int) -> pa.Table:
    return pa.Table.from_batches(list(batches_for_tree(tree, branches, chunk_size)))


def schema_for_branches(tree, branches: list[str]) -> pa.Schema:
    return pa.schema([
        pa.field(branch, pa.from_numpy_dtype(branch_numpy_dtype(tree[branch])))
        for branch in branches
    ])


def ensure_match_branches(tree, branches: list[str]) -> None:
    missing = [branch for branch in MATCH_BRANCHES if branch not in branches]
    if missing:
        raise RuntimeError(
            "--match-event-run-numbers requires branches to be present in every input tree: "
            + ", ".join(missing)
        )


def add_event_run_hash_column(table: pa.Table) -> pa.Table:
    event_run_hash = compute_event_run_hash(table[EVENT_NUMBER_BRANCH], table[RUN_NUMBER_BRANCH])
    if EVENT_RUN_HASH_BRANCH in table.column_names:
        table = table.drop_columns([EVENT_RUN_HASH_BRANCH])
    return table.append_column(EVENT_RUN_HASH_BRANCH, event_run_hash)


def missing_like_array(dtype: pa.DataType, length: int) -> pa.Array:
    if pa.types.is_floating(dtype):
        return pa.array([math.nan] * length, type=dtype)
    return pa.nulls(length, type=dtype)


def temp_ipc_path() -> str:
    temp_file = tempfile.NamedTemporaryFile(suffix=".arrow", delete=False)
    temp_file.close()
    return temp_file.name


def write_record_batches(path: str, schema: pa.Schema, batches: Iterable[pa.RecordBatch]) -> None:
    batch_count = 0
    row_count = 0
    with pa.OSFile(path, "wb") as sink:
        with pa_ipc.RecordBatchFileWriter(sink, schema) as writer:
            for batch in batches:
                writer.write_batch(batch)
                batch_count += 1
                row_count += batch.num_rows
    print(f"[profile] wrote_record_batches path={path} batches={batch_count} rows={row_count}")


def read_record_batches(path: str) -> Iterable[pa.RecordBatch]:
    with pa.memory_map(path, "r") as source:
        reader = pa_ipc.RecordBatchFileReader(source)
        for index in range(reader.num_record_batches):
            yield reader.get_batch(index)


def combine_record_batches(path: str) -> pa.Table:
    return pa.Table.from_batches(list(read_record_batches(path)))


def unique_sorted_hashes(arr: pa.Array) -> pa.Array:
    hash_list = arr.to_pylist()
    if not hash_list:
        return pa.array([], type=pa.int64())
    hash_list.sort()
    unique = [hash_list[0]]
    for value in hash_list[1:]:
        if value != unique[-1]:
            unique.append(value)
    return pa.array(unique, type=pa.int64())


def build_union_hashes_file(trees: list, chunk_size: int) -> str:
    PROFILER.start("build_union_hashes_file")
    chunk_paths: list[str] = []
    hash_schema = pa.schema([pa.field(EVENT_RUN_HASH_BRANCH, pa.int64())])
    input_batches = 0
    input_rows = 0
    try:
        for tree in trees:
            for batch in tree.iterate(expressions=MATCH_BRANCHES, library="np", step_size=chunk_size):
                input_batches += 1
                input_rows += len(batch[EVENT_NUMBER_BRANCH])
                record_batch = numpy_batch_to_record_batch(batch, MATCH_BRANCHES)
                hashes = compute_event_run_hash(
                    record_batch[EVENT_NUMBER_BRANCH],
                    record_batch[RUN_NUMBER_BRANCH],
                )
                unique_hashes = unique_sorted_hashes(hashes)
                chunk_path = temp_ipc_path()
                chunk_paths.append(chunk_path)
                write_record_batches(
                    chunk_path,
                    hash_schema,
                    [pa.record_batch([unique_hashes], names=[EVENT_RUN_HASH_BRANCH])],
                )
        all_hash_batches = []
        for chunk_path in chunk_paths:
            all_hash_batches.extend(read_record_batches(chunk_path))
        combined = pa.Table.from_batches(all_hash_batches).combine_chunks()
        union_hashes = unique_sorted_hashes(combined[EVENT_RUN_HASH_BRANCH])
        output_path = temp_ipc_path()
        write_record_batches(
            output_path,
            hash_schema,
            [pa.record_batch([union_hashes], names=[EVENT_RUN_HASH_BRANCH])],
        )
        PROFILER.stop(
            "build_union_hashes_file",
            input_batches=input_batches,
            input_rows=input_rows,
            unique_hashes=len(union_hashes),
            chunk_files=len(chunk_paths),
        )
        return output_path
    finally:
        for chunk_path in chunk_paths:
            Path(chunk_path).unlink(missing_ok=True)


def iter_union_hashes(path: str, chunk_size: int) -> Iterable[list[int]]:
    for batch in read_record_batches(path):
        hashes = batch[EVENT_RUN_HASH_BRANCH].to_pylist()
        for offset in range(0, len(hashes), chunk_size):
            yield hashes[offset : offset + chunk_size]


def build_sorted_branch_value_file(tree, branches: list[str], chunk_size: int) -> str:
    profile_name = f"build_sorted_branch_value_file[{len(branches)}]"
    PROFILER.start(profile_name)
    read_branches = list(dict.fromkeys([*branches, *MATCH_BRANCHES]))
    chunk_paths: list[str] = []
    schema = pa.schema([pa.field(EVENT_RUN_HASH_BRANCH, pa.int64()), *schema_for_branches(tree, branches)])
    input_batches = 0
    input_rows = 0
    try:
        for batch in tree.iterate(expressions=read_branches, library="np", step_size=chunk_size):
            input_batches += 1
            input_rows += len(batch[EVENT_NUMBER_BRANCH])
            record_batch = numpy_batch_to_record_batch(batch, read_branches)
            arrays = [
                compute_event_run_hash(
                    record_batch[EVENT_NUMBER_BRANCH],
                    record_batch[RUN_NUMBER_BRANCH],
                )
            ] + [record_batch[branch] for branch in branches]
            sorted_table = pa.table(arrays, names=[EVENT_RUN_HASH_BRANCH, *branches]).sort_by(
                [(EVENT_RUN_HASH_BRANCH, "ascending")]
            )
            chunk = pa.record_batch(
                [sorted_table[column_name].combine_chunks() for column_name in schema.names],
                names=schema.names,
            )
            chunk_path = temp_ipc_path()
            chunk_paths.append(chunk_path)
            write_record_batches(chunk_path, schema, [chunk])
        sorted_path = temp_ipc_path()
        merged_batches = []
        for chunk_path in chunk_paths:
            merged_batches.extend(read_record_batches(chunk_path))
        merged = pa.Table.from_batches(merged_batches).sort_by([(EVENT_RUN_HASH_BRANCH, "ascending")])
        write_record_batches(sorted_path, schema, merged.to_batches(max_chunksize=chunk_size))
        PROFILER.stop(
            profile_name,
            input_batches=input_batches,
            input_rows=input_rows,
            chunk_files=len(chunk_paths),
            output_rows=len(merged),
        )
        return sorted_path
    finally:
        for chunk_path in chunk_paths:
            Path(chunk_path).unlink(missing_ok=True)


def rows_by_hash_from_sorted_batch(batch: pa.RecordBatch, branches: list[str]) -> dict[int, tuple[object, ...]]:
    hash_values = batch[EVENT_RUN_HASH_BRANCH].to_pylist()
    branch_arrays = {branch: batch[branch].to_pylist() for branch in branches}
    rows: dict[int, tuple[object, ...]] = {}
    for index, event_run_hash in enumerate(hash_values):
        rows.setdefault(event_run_hash, tuple(branch_arrays[branch][index] for branch in branches))
    return rows


def aligned_batches_for_tree(
    tree,
    branches: list[str],
    union_hash_path: str,
    chunk_size: int,
) -> Iterable[pa.RecordBatch]:
    sorted_values_path = build_sorted_branch_value_file(tree, branches, chunk_size)
    try:
        schema = schema_for_branches(tree, branches)
        value_batches = iter(read_record_batches(sorted_values_path))
        current_batch = next(value_batches, None)
        current_rows = rows_by_hash_from_sorted_batch(current_batch, branches) if current_batch is not None else {}
        current_hashes = sorted(current_rows)
        current_index = 0

        for hash_chunk in iter_union_hashes(union_hash_path, chunk_size):
            batch_arrays = [[] for _ in branches]
            for event_run_hash in hash_chunk:
                while current_batch is not None and current_index >= len(current_hashes):
                    current_batch = next(value_batches, None)
                    current_rows = rows_by_hash_from_sorted_batch(current_batch, branches) if current_batch is not None else {}
                    current_hashes = sorted(current_rows)
                    current_index = 0
                if current_batch is not None and current_index < len(current_hashes) and current_hashes[current_index] == event_run_hash:
                    row = current_rows[event_run_hash]
                    current_index += 1
                else:
                    row = tuple(None for _ in branches)
                for field_index, field in enumerate(schema):
                    value = row[field_index]
                    if value is None and pa.types.is_floating(field.type):
                        value = math.nan
                    batch_arrays[field_index].append(value)
            yield pa.record_batch(
                [pa.array(values, type=schema[field_index].type) for field_index, values in enumerate(batch_arrays)],
                names=branches,
            )
    finally:
        Path(sorted_values_path).unlink(missing_ok=True)


def tables_for_inputs(
    input_paths: list[str],
    tree_name: str,
    branches: list[str] | None,
    chunk_size: int,
    match_event_run_numbers: bool,
) -> list[pa.Table]:
    trees = [open_tree(input_path, tree_name) for input_path in input_paths]
    branch_lists = [filter_supported_branches(tree, branches) for tree in trees]
    if not match_event_run_numbers:
        expected_branches = branch_lists[0]
        for current_branches in branch_lists[1:]:
            if current_branches != expected_branches:
                raise RuntimeError("Input ROOT files do not expose the same supported branches")
        return [table_for_tree(tree, expected_branches, chunk_size) for tree in trees]

    raise RuntimeError("Streaming event/run matching is only implemented for --store-sys-diffs")


def relative_difference_array(systematic: pa.Array, nominal: pa.Array) -> pa.Array:
    sys_values = systematic.to_pylist()
    nom_values = nominal.to_pylist()
    diffs = []
    for sys_value, nom_value in zip(sys_values, nom_values, strict=True):
        if sys_value is None or nom_value is None:
            diffs.append(None)
            continue
        sys_float = float(sys_value)
        nom_float = float(nom_value)
        if math.isfinite(nom_float) and abs(nom_float) > REL_DIFF_EPSILON:
            diffs.append((sys_float - nom_float) / nom_float)
        elif sys_float == nom_float:
            diffs.append(0.0)
        else:
            diffs.append(None)
    return pa.array(diffs, type=pa.float64())


def split_nominal_branch_name(name: str) -> tuple[str, str]:
    if "." not in name:
        raise RuntimeError(f"Expected a nominal branch of the form 'Container.branch', got: {name}")
    container, branch = name.split(".", 1)
    return container, branch


def collect_systematic_mapping(tree, nominal_branches: list[str]) -> tuple[list[str], dict[str, dict[str, str | None]]]:
    nominal_info = {}
    for nominal_branch in nominal_branches:
        container, branch = split_nominal_branch_name(nominal_branch)
        nominal_info[nominal_branch] = (container.removesuffix("AuxDyn"), branch)

    systematics: dict[str, dict[str, str]] = {}
    for name, branch_obj in tree.items():
        if branch_numpy_dtype(branch_obj) is None or "." not in name:
            continue
        container, branch = name.split(".", 1)
        if not container.endswith("AuxDyn"):
            continue
        base_container = container[: container.rfind("AuxDyn")]
        for nominal_branch, (nominal_container_base, nominal_leaf) in nominal_info.items():
            prefix = f"{nominal_container_base}_"
            if branch != nominal_leaf:
                continue
            if not base_container.startswith(prefix):
                continue
            systematic_name = base_container[len(prefix):]
            if not systematic_name:
                continue
            systematics.setdefault(systematic_name, {})[nominal_branch] = name

    if not systematics:
        raise RuntimeError("No systematic variations matching the nominal branches were found")

    completed_systematics: dict[str, dict[str, str | None]] = {}
    nominal_branch_set = set(nominal_branches)
    for systematic_name, mapping in systematics.items():
        missing = sorted(nominal_branch_set - set(mapping))
        if missing:
            print(
                f"Warning: systematic '{systematic_name}' is missing nominal branches; filling reldiff outputs with nan for: "
                + ", ".join(missing)
            )
        completed_systematics[systematic_name] = {
            nominal_branch: mapping.get(nominal_branch) for nominal_branch in nominal_branches
        }
    return sorted(completed_systematics), completed_systematics


def reldiff_dtype(quantize_systs: str | None) -> pa.DataType:
    if quantize_systs == "float32":
        return pa.float32()
    if quantize_systs == "float16":
        return pa.float16()
    if quantize_systs == "int16":
        return pa.int16()
    return pa.float64()


def nan_array(length: int, dtype: pa.DataType) -> pa.Array:
    if pa.types.is_integer(dtype):
        return pa.array([INT16_MISSING_SENTINEL] * length, type=dtype)
    return pa.array([math.nan] * length, type=dtype)


def quantize_array(values: pa.Array, dtype: pa.DataType) -> pa.Array:
    if values.type == dtype:
        return values
    if pa.types.is_integer(dtype):
        python_values = values.to_pylist()
        quantized = []
        lower = -(1 << 15) + 1
        upper = (1 << 15) - 1
        for value in python_values:
            if value is None or not math.isfinite(value):
                quantized.append(INT16_MISSING_SENTINEL)
                continue
            scaled = int(round(value * INT16_QUANTIZATION_SCALE))
            scaled = max(lower, min(upper, scaled))
            quantized.append(scaled)
        return pa.array(quantized, type=dtype)
    return pc.cast(values, dtype)


def sys_diff_schema(
    nominal_tree,
    nominal_branches: list[str],
    systematic_names: list[str],
    quantize_systs: str | None,
) -> pa.Schema:
    systematic_dtype = reldiff_dtype(quantize_systs)
    return pa.schema(
        [
            pa.field(EVENT_NUMBER_BRANCH, pa.uint64()),
            pa.field(RUN_NUMBER_BRANCH, pa.uint32()),
            pa.field(EVENT_RUN_HASH_BRANCH, pa.int64()),
            *schema_for_branches(nominal_tree, nominal_branches),
            *[
                pa.field(f"{nominal_branch}__{systematic_name}__reldiff", systematic_dtype)
                for systematic_name in systematic_names
                for nominal_branch in nominal_branches
            ],
        ]
    )


def sys_diff_batches(
    nominal_path: str,
    systematic_paths: list[str],
    tree_name: str,
    branches: list[str] | None,
    chunk_size: int,
    match_event_run_numbers: bool,
    quantize_systs: str | None,
) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
    nominal_tree = open_tree(nominal_path, tree_name)
    systematic_trees = [open_tree(systematic_path, tree_name) for systematic_path in systematic_paths]

    nominal_candidates = filter_supported_branches(nominal_tree, branches)
    nominal_branches = [name for name in nominal_candidates if name.startswith(f"{NOMINAL_CONTAINER}.")]
    if branches is not None:
        missing_nominal = sorted(set(branches) - set(nominal_branches))
        if missing_nominal:
            raise RuntimeError(
                "With --store-sys-diffs, requested branches must be nominal HGamEventInfoAuxDyn branches: "
                + ", ".join(missing_nominal)
            )
    if not nominal_branches:
        raise RuntimeError("No supported nominal HGamEventInfoAuxDyn branches found")

    systematic_dtype = reldiff_dtype(quantize_systs)
    systematic_names: list[str] = []
    systematic_mappings: list[tuple[list[str], dict[str, dict[str, str | None]]]] = []
    systematic_needed_branches_per_tree: list[list[str]] = []
    for systematic_tree in systematic_trees:
        tree_systematic_names, systematic_mapping = collect_systematic_mapping(systematic_tree, nominal_branches)
        systematic_names.extend(tree_systematic_names)
        systematic_mappings.append((tree_systematic_names, systematic_mapping))
        systematic_needed_branches_per_tree.append(
            sorted(
                {
                    systematic_branch
                    for systematic_name in tree_systematic_names
                    for nominal_branch in nominal_branches
                    for systematic_branch in [systematic_mapping[systematic_name][nominal_branch]]
                    if systematic_branch is not None
                }
            )
        )
    schema = sys_diff_schema(nominal_tree, nominal_branches, systematic_names, quantize_systs)

    if match_event_run_numbers:
        ensure_match_branches(nominal_tree, nominal_candidates)
        for systematic_tree in systematic_trees:
            ensure_match_branches(systematic_tree, filter_supported_branches(systematic_tree, None))
        union_hash_path = build_union_hashes_file([nominal_tree, *systematic_trees], chunk_size)

        def generator() -> Iterable[pa.RecordBatch]:
            try:
                nominal_match_batches = aligned_batches_for_tree(
                    nominal_tree,
                    [EVENT_NUMBER_BRANCH, RUN_NUMBER_BRANCH],
                    union_hash_path,
                    chunk_size,
                )
                nominal_batches = aligned_batches_for_tree(nominal_tree, nominal_branches, union_hash_path, chunk_size)
                systematic_batch_iters = [
                    aligned_batches_for_tree(systematic_tree, needed_branches, union_hash_path, chunk_size)
                    for systematic_tree, needed_branches in zip(
                        systematic_trees,
                        systematic_needed_branches_per_tree,
                        strict=True,
                    )
                ]
                for combined_batches in zip(
                    nominal_match_batches,
                    nominal_batches,
                    *systematic_batch_iters,
                    strict=True,
                ):
                    match_batch = combined_batches[0]
                    nominal_batch = combined_batches[1]
                    systematic_batches = combined_batches[2:]
                    event_numbers = match_batch[EVENT_NUMBER_BRANCH]
                    run_numbers = match_batch[RUN_NUMBER_BRANCH]
                    event_run_hash = compute_event_run_hash(event_numbers, run_numbers)
                    arrays = [event_numbers, run_numbers, event_run_hash, *[nominal_batch[nominal_branch] for nominal_branch in nominal_branches]]
                    names = [EVENT_NUMBER_BRANCH, RUN_NUMBER_BRANCH, EVENT_RUN_HASH_BRANCH, *nominal_branches]
                    row_count = nominal_batch.num_rows
                    for (tree_systematic_names, systematic_mapping), systematic_batch in zip(
                        systematic_mappings,
                        systematic_batches,
                        strict=True,
                    ):
                        for systematic_name in tree_systematic_names:
                            for nominal_branch in nominal_branches:
                                systematic_branch = systematic_mapping[systematic_name][nominal_branch]
                                nominal_array = nominal_batch[nominal_branch]
                                if systematic_branch is None or systematic_branch not in systematic_batch.schema.names:
                                    arrays.append(nan_array(row_count, systematic_dtype))
                                else:
                                    systematic_array = systematic_batch[systematic_branch]
                                    arrays.append(
                                        quantize_array(
                                            relative_difference_array(systematic_array, nominal_array),
                                            systematic_dtype,
                                        )
                                    )
                                names.append(f"{nominal_branch}__{systematic_name}__reldiff")
                    yield pa.record_batch(arrays, names=names)
            finally:
                Path(union_hash_path).unlink(missing_ok=True)

        return schema, generator()

    nominal_match_iter = batches_for_tree(nominal_tree, [EVENT_NUMBER_BRANCH, RUN_NUMBER_BRANCH], chunk_size)
    nominal_iter = batches_for_tree(nominal_tree, nominal_branches, chunk_size)
    systematic_batch_iters = [
        batches_for_tree(systematic_tree, needed_branches, chunk_size)
        for systematic_tree, needed_branches in zip(
            systematic_trees,
            systematic_needed_branches_per_tree,
            strict=True,
        )
    ]

    def generator() -> Iterable[pa.RecordBatch]:
        for combined_batches in zip(
            nominal_match_iter,
            nominal_iter,
            *systematic_batch_iters,
            strict=True,
        ):
            match_batch = combined_batches[0]
            nominal_batch = combined_batches[1]
            systematic_batches = combined_batches[2:]
            event_numbers = match_batch[EVENT_NUMBER_BRANCH]
            run_numbers = match_batch[RUN_NUMBER_BRANCH]
            event_run_hash = compute_event_run_hash(event_numbers, run_numbers)
            arrays = [event_numbers, run_numbers, event_run_hash, *[nominal_batch[nominal_branch] for nominal_branch in nominal_branches]]
            names = [EVENT_NUMBER_BRANCH, RUN_NUMBER_BRANCH, EVENT_RUN_HASH_BRANCH, *nominal_branches]
            row_count = nominal_batch.num_rows
            for systematic_batch in systematic_batches:
                if nominal_batch.num_rows != systematic_batch.num_rows:
                    raise RuntimeError("Nominal and systematic chunk sizes do not align")
            for (tree_systematic_names, systematic_mapping), systematic_batch in zip(
                systematic_mappings,
                systematic_batches,
                strict=True,
            ):
                for systematic_name in tree_systematic_names:
                    for nominal_branch in nominal_branches:
                        systematic_branch = systematic_mapping[systematic_name][nominal_branch]
                        nominal_array = nominal_batch[nominal_branch]
                        if systematic_branch is None or systematic_branch not in systematic_batch.schema.names:
                            arrays.append(nan_array(row_count, systematic_dtype))
                        else:
                            systematic_array = systematic_batch[systematic_branch]
                            arrays.append(
                                quantize_array(
                                    relative_difference_array(systematic_array, nominal_array),
                                    systematic_dtype,
                                )
                            )
                        names.append(f"{nominal_branch}__{systematic_name}__reldiff")
            yield pa.record_batch(arrays, names=names)

    return schema, generator()


def concatenate_tables(tables: list[pa.Table]) -> pa.Table:
    if len(tables) == 1:
        return tables[0]
    return pa.concat_tables(tables)


def write_vortex(data: pa.Table | pa.RecordBatchReader, output_path: str) -> None:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    PROFILER.start("write_vortex")
    vortex.io.write(data, str(output))
    size_bytes = output.stat().st_size if output.exists() else 0
    PROFILER.stop("write_vortex", output_path=output_path, size_bytes=size_bytes)


def main() -> None:
    args = parse_args()
    PROFILER.start("total")

    if args.chunk_size <= 0:
        raise RuntimeError("--chunk-size must be positive")
    if args.store_sys_diffs and len(args.inputs) < 2:
        raise RuntimeError(
            "--store-sys-diffs requires at least two inputs: NOMINAL_ROOT SYSTEMATIC_ROOT [SYSTEMATIC_ROOT ...]"
        )

    if args.store_sys_diffs:
        schema, batches = sys_diff_batches(
            args.inputs[0],
            args.inputs[1:],
            args.tree,
            args.branches,
            args.chunk_size,
            args.match_event_run_numbers,
            args.quantize_systs,
        )
        data = pa.RecordBatchReader.from_batches(schema, batches)
    else:
        tables = tables_for_inputs(
            args.inputs,
            args.tree,
            args.branches,
            args.chunk_size,
            args.match_event_run_numbers,
        )
        data = concatenate_tables(tables)

    write_vortex(data, args.output)
    output_size_bytes = Path(args.output).stat().st_size if Path(args.output).exists() else 0
    PROFILER.stop("total", output_path=args.output, size_bytes=output_size_bytes)


if __name__ == "__main__":
    main()
