# Stage 3 — publish runbook (R4)

Publishing is **irreversible** (crates.io versions are permanent). Each step below needs David's explicit
go. Dry-run status current as of the R4 lining-up pass. Names verified free: `timsim-types`,
`timsim-schema`, `timsim-chem`, `timsim-core`; `ms-io` has only `0.1.0` published (0.2.0 open).

## Order (a crate can't publish before its deps are on crates.io)

| # | crate | from | deps status | dry-run | change needed first |
|---|-------|------|-------------|---------|---------------------|
| 1 | **ms-io 0.2.0** | `/scratch/timsim-demo/mscore` | mscore 0.5.0 ✅ published | ✅ clean (31 files) | none |
| 2 | **timsim-types 0.1.0** | `/scratch/timsim-demo/timsim` | none (zero-dep) | ✅ clean (5 files) | none |
| 3 | **timsim-schema 0.1.0** | `/scratch/timsim-demo/timsim` | arrow/parquet/thiserror ✅ | ✅ clean | none |
| 4 | **timsim-chem 0.1.0** | `/scratch/timsim-demo/timsim` | rayon/regex ✅ (+published dev) | ✅ clean (20 files) | none |
| 5 | **timsim-core 0.1.0** | `/scratch/timsim-demo/timsim` | **ms-io 0.2.0 + timsim-types 0.1.0** | ⏳ after #1,#2 | flip path→version (below) |

Steps 1–4 are independent and can go in any order; **5 must be last** (it depends on 1 + 2).

## Commands

```bash
# 1
cd /scratch/timsim-demo/mscore   && cargo publish -p ms-io
# 2,3,4  (independent)
cd /scratch/timsim-demo/timsim   && cargo publish -p timsim-types
cd /scratch/timsim-demo/timsim   && cargo publish -p timsim-schema
cd /scratch/timsim-demo/timsim   && cargo publish -p timsim-chem
```

### Step 5 — timsim-core (only after ms-io 0.2.0 + timsim-types 0.1.0 are live on crates.io)
crates.io rejects path deps, so flip `timsim-core/Cargo.toml`:
- `ms-io = { path = "../../mscore/ms-io" }`  →  `ms-io = "0.2.0"`
- `timsim-types = { path = "../timsim-types" }`  →  `timsim-types = "0.1.0"`

Keep local dev building against the working tree — add to the **timsim workspace** `[patch.crates-io]`:
```toml
ms-io = { path = "../mscore/ms-io" }
timsim-types = { path = "timsim-types" }
```
Then:
```bash
cd /scratch/timsim-demo/timsim && cargo publish -p timsim-core   # verify build resolves the published deps
git add timsim-core/Cargo.toml Cargo.toml && git commit -m "timsim-core: version deps for publish + [patch] for local dev"
```

## Post-publish (optional polish — path deps already work locally)
Flip the **rustims** consumers from path → version + `[patch.crates-io]`, matching the ms-chem/mscore pattern:
- `sciex-io`: `timsim-types = "0.1.0"`
- `imspy_connector`: `timsim-core = "0.1.0"`, `ms-io = "0.2.0"`
- `timsim-cli`: `timsim-chem`/`timsim-schema`/`timsim-core` → versions, `ms-io = "0.2.0"`
- `imsjl_connector` / `tims-viewer`: `ms-io = "0.2.0"`
- rustims `[patch.crates-io]`: add `ms-io`, `timsim-types`, `timsim-schema`, `timsim-chem`, `timsim-core`
  → sibling checkouts (`../../mscore/*`, `../../../timsim/*` — mind nested-crate path levels).

## Result
`ms-chem`, `mscore`, `ms-io`, `timsim-types`, `timsim-schema`, `timsim-chem`, `timsim-core` all on
crates.io → anyone can `cargo add timsim-core`. `sciex-io` stays rustims-local (private sciexwiff, held).
