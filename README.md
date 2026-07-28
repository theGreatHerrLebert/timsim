# timsim

The Rust core of the **timsim v2** proteomics simulator: four crates that go from a FASTA to a
rendered DIA/DDA run, with a typed column contract in between. Published on crates.io, MIT-licensed,
`rust-version = 1.84`.

The orchestration (the necroflow DAG), the CLI binaries, and the deep predictors live in their own
repositories — this workspace is the library layer they all build on.

## The crates

| crate | what it is |
|---|---|
| [`timsim-types`](timsim-types) | **Zero-dependency** contract types: the vendor-neutral DIA `AcquisitionScheme` — an ordered sequence of physical `AcquisitionEvent`s (an MS2 frame carries a `Vec<DiaWindow>`, so a mobility-partitioned timsTOF frame and a linear Astral/SCIEX scan are both expressible), plus windows, mobility geometry and activation/CE policy. Pure `std`, so anything can name a scheme without pulling Arrow or an I/O stack. |
| [`timsim-schema`](timsim-schema) | The **Arrow/Parquet column contract**: 17 tables across four axes (structure / quantity / design / measurement), a `SCHEMA_VERSION`, and — the point of the crate — **validation on read** (`read`, `read_stream`, `validate`) so a stage rejects a wrong input *before* it computes, with a column-level message. Column names are constants (`peptides::MASS_MONOISOTOPIC`), never string literals in stage code. |
| [`timsim-chem`](timsim-chem) | ~6k LOC of **sample chemistry**, instrument-free: FASTA parsing and cleavage rules derived from Sage; a digest split along the structure/quantity boundary (`Enumerator` = which peptides exist, `YieldModel` = how much of each); occupancy-based modforms (no "fixed vs variable" — only site occupancy, truncated on probability mass); experimental `design`/mixtures where fold changes are *derived* from the mixture and mass balance; `ionize` (charge/flyability); exact convolutional isotope envelopes from the real composition (not averagine); b/y fragments; and content-hash IDs with explicit collision detection. |
| [`timsim-core`](timsim-core) | ~11k LOC **simulation engine** (extracted from the former `ms-io::sim`): DIA and DDA frame builders plus memory-bounded *lazy* builders that load only the peptides for the frames being written; the `projector` (EMG retention-time profile integrated over each event's true interval, ion-mobility Gaussian sampled onto the scan grid, marginalised for non-IMS instruments); `scheme` adapters that read/write a real `.d`/`.raw`'s acquisition schedule; the `AcquisitionWriter` trait with `ThermoRawWriter`; `astral_dispatch`; an mzML writer; and a parallel spectral-library (DiaNN/Spectronaut transition TSV) writer. |

## Build

```bash
git clone https://github.com/theGreatHerrLebert/timsim
cd timsim
cargo build --workspace
```

That is the whole story — every dependency resolves from crates.io and **no sibling checkout is
required**. If you only want one crate: `cargo add timsim-core`.

**A C toolchain is required.** `timsim-core` depends on `rusqlite` with the `bundled` feature (it
writes Bruker `analysis.tdf`), which compiles SQLite from source.

### Optional features (both OFF by default)

`timsim-core` gates its two non-Bruker writers behind features, so the default build stays lean:

```bash
cargo build -p timsim-core --features thermo   # Thermo .raw writer (thermorawfile) + astral_dispatch
cargo build -p timsim-core --features mzml     # open mzML writer (mzdata)
```

The `thermo_m0_survival` / `thermo_m1_seam` examples require `--features thermo`.

### Local development against the foundation crates

`ms-chem`, `mscore` and `ms-io` are published, so nothing here needs them checked out. If you *are*
editing them alongside this workspace, add the override to a **git-ignored `.cargo/config.toml`** at
the repo root rather than to `Cargo.toml`:

```toml
# .cargo/config.toml — local only, never committed
[patch.crates-io]
ms-chem = { path = "../mscore/ms-chem" }
mscore  = { path = "../mscore/mscore" }
ms-io   = { path = "../mscore/ms-io" }
```

Cargo honours `[patch]` from config exactly as it does from a manifest. Keeping it out of the
committed manifest is deliberate: a patch pointing at `../mscore` makes `git clone && cargo build`
fail for everyone who does not happen to have that sibling repo, and a convenience must never be a
precondition. `.cargo/` is already in `.gitignore`.

## Tests and validation

```bash
cargo test --workspace
```

`timsim-chem` carries cross-implementation parity tests against the published `ms-chem` / `mscore`
(dev-dependencies only — they are not in the build graph of the library).

**`timsim-chem/xcheck/` is a separate workspace**, not a member of this one, and is not tracked in
this repository. It is an on-demand validation harness that cross-checks the digest against
[Sage](https://github.com/lazear/sage); `sage-core` is an entire search engine and has no business
in the dependency graph of `timsim-chem`. Run it on its own (`cd timsim-chem/xcheck && cargo run`)
after placing it there.

## Layout

```
timsim-types/    the zero-dependency acquisition-scheme leaf
timsim-schema/   tables.rs (the column registry) + lib.rs (write/read/validate)
timsim-chem/     fasta, enzyme, digest, modify, design, ionize, isotope, fragment, ids, mass
timsim-core/     dia, dda, lazy_builder, projector, scheme, acquisition, astral_dispatch,
                 mzml, library, handle, containers, precursor, utility
PUBLISHING.md    the crates.io release runbook (order + dependency constraints)
```

## Licence

MIT. Cleavage rules and FASTA parsing in `timsim-chem` are derived from Sage
(MIT, Copyright (c) 2022 Michael Lazear).
