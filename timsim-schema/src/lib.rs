//! The schema contract for timsim v2.
//!
//! # The failure this exists to prevent
//!
//! > "You push one button down and three stages downstream the program explodes because a
//! > hard-coded column name is no longer GRU."
//!
//! In v1 the schema is an **implicit contract, enforced nowhere** until something reads a
//! column that isn't there. `retention_time_gru_predictor` — named after a model replaced
//! years ago — is hard-coded in the Rust SQL reader in 66 places across two languages, and
//! Python has degraded to searching for it by substring.
//!
//! Moving to Parquet does not fix that. Two things do, and only these:
//!
//! 1. **One schema definition** ([`tables`]). A column name is never a string literal in
//!    stage code. This crate is the Rust half of that contract — the Python stages (the deep
//!    predictors `timsim-rt` / `timsim-ccs`) read the same column names through their own
//!    reader, which lives in a separate repository. There is no Python in this workspace.
//! 2. **Validate on read, at every stage boundary** ([`read`]). A tool asserts its input
//!    conforms *before* it computes — so the explosion happens at the stage with the wrong
//!    input, with a column-level message, rather than three stages later inside a `row.get()`.
//!
//! Necroflow adds a third layer above both: typed `NodeType` edges are checked at
//! rule-call time, so a mis-wiring fails before the DAG executes at all.

use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

pub mod tables;
pub use tables::SCHEMA_VERSION;

/// Which axis of the model a table belongs to. Recorded in the file so an artifact is
/// self-describing, and so a structure table can never be silently fed a quantity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Axis {
    /// What molecules exist. Computed once; shared by every sample in a design.
    Structure,
    /// How much of each, per sample. Cheap; recomputed per condition.
    Quantity,
    /// Samples, runs, and the mapping between them.
    Design,
    /// How it is observed. Per run.
    Measurement,
}

impl Axis {
    pub fn as_str(&self) -> &'static str {
        match self {
            Axis::Structure => "structure",
            Axis::Quantity => "quantity",
            Axis::Design => "design",
            Axis::Measurement => "measurement",
        }
    }
}

#[derive(Clone, Debug)]
pub struct TableSpec {
    pub name: &'static str,
    pub axis: Axis,
    pub schema: SchemaRef,
}

// ─────────────────────────────────────────────────────────────────────────────

/// Arrow key-value metadata keys stamped into every artifact.
pub mod meta {
    pub const VERSION: &str = "timsim.schema_version";
    pub const TABLE: &str = "timsim.table";
    pub const AXIS: &str = "timsim.axis";
    pub const PRODUCER: &str = "timsim.producer";
    /// The declared validity scope of a structure artifact — the LC chemistry family and the
    /// source reference state it was built for. The structure axis is **not "device-free"**;
    /// it is invariant under a *declared equivalence class of methods*. A structure built for
    /// C18 must not be silently consumed by a HILIC method.
    pub const SCOPE: &str = "timsim.scope";

    /// The enumeration bounds a structure artifact was built under.
    ///
    /// These live in the artifact because a consumer must NOT re-enter them. `timsim-yield`
    /// originally took its own `--max-missed-cleavages`; digesting at 4 and yielding at the
    /// default 2 silently gave every 3- and 4-missed-cleavage occurrence a **zero yield**.
    /// A fact the artifact already knows must be read, never retyped.
    /// Declared **technical** (injection-to-injection) CV.
    ///
    /// A technical replicate is the *same tube injected twice*, so its amounts are identical and all
    /// of its variation belongs to the **measurement** axis. The design therefore cannot apply it —
    /// but it must not silently swallow it either. It travels with the design artifact so the
    /// measurement stage READS it rather than the user re-entering it (B13).
    pub const VARIANCE_TECHNICAL: &str = "timsim.variance.technical";

    pub const MAX_MISSED_CLEAVAGES: &str = "timsim.bounds.max_missed_cleavages";
    pub const MIN_LENGTH: &str = "timsim.bounds.min_length";
    pub const MAX_LENGTH: &str = "timsim.bounds.max_length";
}

#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("unknown table {0:?}; known tables: {1}")]
    UnknownTable(String, String),

    #[error(
        "{path}: not a timsim artifact — {missing} is absent.\n  \
         A Parquet file whose columns happen to line up is not proof of identity. Produce this \
         file with a timsim tool, or stamp it."
    )]
    Unstamped { path: String, missing: &'static str },

    #[error(
        "{path}: this file is a {found:?} table, but {expected:?} was expected.\n  \
         The wrong artifact was wired into this stage."
    )]
    WrongTable {
        path: String,
        found: String,
        expected: String,
    },

    #[error(
        "{path}: schema version {found} is incompatible with {expected} (major versions must match)"
    )]
    IncompatibleVersion {
        path: String,
        found: String,
        expected: String,
    },

    /// The GRU error, caught at the boundary instead of three stages downstream.
    #[error(
        "{path}: table {table:?} does not conform to schema {version}.\n{report}\
         \n  This is a schema mismatch at a stage boundary, not a bug in this stage."
    )]
    Nonconforming {
        path: String,
        table: String,
        version: String,
        report: String,
    },
}

/// Type equality that ignores the *name* of a List's inner field. Arrow names list elements
/// "item" (arrow-rs, our writer) or "element" (parquet spec, what pyarrow's writer emits); the two
/// are semantically identical, so a Parquet file that round-tripped through pandas/pyarrow must not
/// be rejected purely for saying "element". The value type and its nullability still matter and are
/// compared recursively (so a genuine element-type mismatch is still caught).
fn types_compatible(found: &DataType, want: &DataType) -> bool {
    match (found, want) {
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b)) => {
            a.is_nullable() == b.is_nullable()
                && types_compatible(a.data_type(), b.data_type())
        }
        _ => found == want,
    }
}

/// A column-level diff, written to be read by a human at 2am.
fn conformance_report(spec: &TableSpec, actual: &SchemaRef) -> Option<String> {
    let actual_types: HashMap<&str, &DataType> = actual
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.data_type()))
        .collect();

    let actual_nullable: HashMap<&str, bool> = actual
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.is_nullable()))
        .collect();

    let mut problems = Vec::new();
    for want in spec.schema.fields() {
        match actual_types.get(want.name().as_str()) {
            None => problems.push(format!(
                "  - missing column {:?} ({})",
                want.name(),
                want.data_type()
            )),
            Some(found) if !types_compatible(found, want.data_type()) => problems.push(format!(
                "  - column {:?} has type {} but the schema declares {}",
                want.name(),
                found,
                want.data_type()
            )),
            Some(_) => {
                // Nullability is part of the contract, not decoration. A required column that
                // arrives nullable will be read with `.value(i)`, which on a null returns
                // garbage rather than failing — a silently wrong number, which is worse than
                // a crash.
                if !want.is_nullable() && actual_nullable[want.name().as_str()] {
                    problems.push(format!(
                        "  - column {:?} is nullable but the schema declares it required",
                        want.name()
                    ));
                }
            }
        }
    }

    // Extra columns are permitted — a stage may annotate without breaking its consumers —
    // but they are surfaced, because an unexpected column is often a renamed one.
    let declared: Vec<&str> = spec.schema.fields().iter().map(|f| f.name().as_str()).collect();
    let extra: Vec<&str> = actual
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .filter(|n| !declared.contains(n))
        .collect();
    if !extra.is_empty() && !problems.is_empty() {
        problems.push(format!(
            "  ? unexpected columns present: {} — is one of these a renamed column?",
            extra.join(", ")
        ));
    }

    if problems.is_empty() {
        None
    } else {
        Some(problems.join("\n"))
    }
}

fn major(v: &str) -> &str {
    v.split('.').next().unwrap_or(v)
}

// ─────────────────────────────────────────────────────────────────────────────

/// The row-group size an artifact is written at — parquet's `DEFAULT_MAX_ROW_GROUP_SIZE`, which is
/// what [`write`] gets by not overriding it. Exposed because a **streaming** producer that wants its
/// output to be byte-identical to a single-batch [`write`] must hand [`Writer`] batches that line up
/// with this boundary; [`Writer`] does that re-alignment itself, and a caller that is already aligned
/// pays nothing for it. See [`Writer`] for why the alignment is what makes the bytes identical.
pub const ROW_GROUP_ROWS: usize = parquet::file::properties::DEFAULT_MAX_ROW_GROUP_SIZE;

/// The compression an artifact is written with. One definition, shared by [`write_with`] and
/// [`Writer`], so a streamed file cannot drift from a single-batch one.
fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
        .build()
}

/// The key-value metadata stamped into every artifact of `table`.
fn stamp_kv(
    spec: &TableSpec,
    table: &str,
    producer: &str,
    scope: Option<&str>,
    extra: &[(&str, String)],
) -> Vec<(String, String)> {
    let mut kv = vec![
        (meta::VERSION.to_string(), SCHEMA_VERSION.to_string()),
        (meta::TABLE.to_string(), table.to_string()),
        (meta::AXIS.to_string(), spec.axis.as_str().to_string()),
        (meta::PRODUCER.to_string(), producer.to_string()),
    ];
    if let Some(s) = scope {
        kv.push((meta::SCOPE.to_string(), s.to_string()));
    }
    for (k, v) in extra {
        kv.push((k.to_string(), v.clone()));
    }
    kv
}

/// Re-stamp a batch's schema with the artifact metadata, preserving the field definitions (so an
/// extra column a stage chose to annotate with survives).
fn stamped_schema(batch_schema: &SchemaRef, kv: &[(String, String)]) -> SchemaRef {
    let mut md: HashMap<String, String> = batch_schema.metadata().clone();
    for (k, v) in kv {
        md.insert(k.clone(), v.clone());
    }
    std::sync::Arc::new(
        arrow::datatypes::Schema::new(batch_schema.fields().clone()).with_metadata(md),
    )
}

fn lookup(table: &str) -> Result<TableSpec, SchemaError> {
    tables::by_name(table).ok_or_else(|| {
        SchemaError::UnknownTable(
            table.to_string(),
            tables::all().iter().map(|t| t.name).collect::<Vec<_>>().join(", "),
        )
    })
}

/// Write a record batch as a timsim artifact, stamping the schema metadata.
pub fn write(
    path: impl AsRef<Path>,
    table: &str,
    batch: &RecordBatch,
    producer: &str,
    scope: Option<&str>,
) -> Result<(), SchemaError> {
    write_with(path, table, batch, producer, scope, &[])
}

/// As [`write`], with extra key-value metadata — used to persist the enumeration bounds onto
/// a structure artifact so consumers read them instead of re-entering them.
pub fn write_with(
    path: impl AsRef<Path>,
    table: &str,
    batch: &RecordBatch,
    producer: &str,
    scope: Option<&str>,
    extra: &[(&str, String)],
) -> Result<(), SchemaError> {
    let spec = lookup(table)?;

    if let Some(report) = conformance_report(&spec, &batch.schema()) {
        return Err(SchemaError::Nonconforming {
            path: path.as_ref().display().to_string(),
            table: table.to_string(),
            version: SCHEMA_VERSION.to_string(),
            report,
        });
    }

    let kv = stamp_kv(&spec, table, producer, scope, extra);
    let schema = stamped_schema(&batch.schema(), &kv);
    let batch = RecordBatch::try_new(schema.clone(), batch.columns().to_vec())?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(writer_properties()))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

/// A **streaming** artifact writer: the same file [`write`] produces, but built a batch at a time so
/// a producer never has to hold the whole table in memory.
///
/// # Why this exists
///
/// [`write`] takes one `RecordBatch`, which forces a stage to materialise every row before a single
/// byte reaches disk. For `ion_spectra` at full-proteome scale that is tens of GB of peak lists held
/// only so they can be handed to the writer once. A stage that can produce its rows in order — and
/// they all can — should be able to stream them out.
///
/// # Why a streamed file is byte-identical to a single-batch one
///
/// Not automatic, and worth stating exactly, because the *content* being equal is not the same claim
/// as the *bytes* being equal — and downstream caching keys on the bytes.
///
/// Parquet page boundaries are decided by a size check performed every `write_batch_size` (1024)
/// **levels**, and the level counter restarts on every call into the column writer. So chopping a
/// table into arbitrary batches moves the check points, which moves the page boundaries, which
/// changes the bytes even though every value is the same. The one place the arrow writer *does*
/// restart that counter identically is at a **row-group boundary**: given one huge batch it splits it
/// into [`ROW_GROUP_ROWS`]-row slices and feeds them one at a time. Feed it exactly those slices and
/// it cannot tell the difference.
///
/// So this writer buffers whatever it is given and only ever hands the underlying writer whole
/// [`ROW_GROUP_ROWS`]-row units (plus the final short one). A caller already aligned to that boundary
/// — the intended case — takes a pass-through path that copies nothing; an unaligned caller pays one
/// concatenation per row group and gets the same bytes anyway.
///
/// ```no_run
/// # fn main() -> Result<(), timsim_schema::SchemaError> {
/// # let batches: Vec<arrow::record_batch::RecordBatch> = vec![];
/// let mut w = timsim_schema::Writer::new("out.parquet", "ion_spectra", "timsim-spectra", None)?;
/// for batch in &batches {
///     w.write(batch)?;
/// }
/// w.close()?;
/// # Ok(()) }
/// ```
pub struct Writer {
    path: std::path::PathBuf,
    table: String,
    spec: TableSpec,
    kv: Vec<(String, String)>,
    /// Created on the first batch, so the file schema keeps that batch's field definitions exactly
    /// as [`write_with`] does (extra columns included).
    inner: Option<ArrowWriter<File>>,
    /// The stamped schema the file declares, fixed by the first batch.
    file_schema: Option<SchemaRef>,
    /// Rows not yet forming a whole row group.
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    rows: u64,
}

impl Writer {
    /// Open a streaming writer. The file is created on the first [`Writer::write`] (or on
    /// [`Writer::close`] if no row is ever written, which yields a valid empty artifact).
    pub fn new(
        path: impl AsRef<Path>,
        table: &str,
        producer: &str,
        scope: Option<&str>,
    ) -> Result<Self, SchemaError> {
        Self::new_with(path, table, producer, scope, &[])
    }

    /// As [`Writer::new`], with the extra key-value metadata [`write_with`] accepts.
    pub fn new_with(
        path: impl AsRef<Path>,
        table: &str,
        producer: &str,
        scope: Option<&str>,
        extra: &[(&str, String)],
    ) -> Result<Self, SchemaError> {
        let spec = lookup(table)?;
        let kv = stamp_kv(&spec, table, producer, scope, extra);
        Ok(Writer {
            path: path.as_ref().to_path_buf(),
            table: table.to_string(),
            spec,
            kv,
            inner: None,
            file_schema: None,
            pending: Vec::new(),
            pending_rows: 0,
            rows: 0,
        })
    }

    /// Rows accepted so far.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Append a batch. Conformance is checked per batch, at the same boundary and with the same
    /// message [`write`] would give.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), SchemaError> {
        if let Some(report) = conformance_report(&self.spec, &batch.schema()) {
            return Err(SchemaError::Nonconforming {
                path: self.path.display().to_string(),
                table: self.table.clone(),
                version: SCHEMA_VERSION.to_string(),
                report,
            });
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.rows += batch.num_rows() as u64;

        // Fast path: nothing buffered, so whole row groups can go straight through — the arrow
        // writer splits them at exactly the boundary we would have split them at, and no data is
        // copied. This is the path the streaming stages take.
        if self.pending_rows == 0 {
            let whole = batch.num_rows() - batch.num_rows() % ROW_GROUP_ROWS;
            if whole > 0 {
                self.emit(&batch.slice(0, whole))?;
            }
            if whole < batch.num_rows() {
                self.pending_rows = batch.num_rows() - whole;
                self.pending.push(batch.slice(whole, self.pending_rows));
            }
            return Ok(());
        }

        self.pending_rows += batch.num_rows();
        self.pending.push(batch.clone());
        while self.pending_rows >= ROW_GROUP_ROWS {
            let group = self.take_pending(ROW_GROUP_ROWS)?;
            self.emit(&group)?;
        }
        Ok(())
    }

    /// Flush the tail and finalise the file.
    pub fn close(mut self) -> Result<(), SchemaError> {
        if self.pending_rows > 0 {
            let tail = self.take_pending(self.pending_rows)?;
            self.emit(&tail)?;
        }
        match self.inner.take() {
            Some(w) => {
                w.close()?;
            }
            // Nothing was ever written: still produce a real, stamped, empty artifact rather than
            // leaving a consumer to trip over a missing file.
            None => {
                let schema = stamped_schema(&self.spec.schema, &self.kv);
                let file = File::create(&self.path)?;
                ArrowWriter::try_new(file, schema, Some(writer_properties()))?.close()?;
            }
        }
        Ok(())
    }

    /// Pull exactly `n` rows off the front of `pending`, concatenated into one batch.
    fn take_pending(&mut self, n: usize) -> Result<RecordBatch, SchemaError> {
        let schema = self.pending[0].schema();
        let mut take: Vec<RecordBatch> = Vec::new();
        let mut left = n;
        while left > 0 {
            let front = self.pending.remove(0);
            if front.num_rows() <= left {
                left -= front.num_rows();
                take.push(front);
            } else {
                take.push(front.slice(0, left));
                self.pending.insert(0, front.slice(left, front.num_rows() - left));
                left = 0;
            }
        }
        self.pending_rows -= n;
        Ok(arrow::compute::concat_batches(&schema, &take)?)
    }

    fn emit(&mut self, batch: &RecordBatch) -> Result<(), SchemaError> {
        if self.inner.is_none() {
            let schema = stamped_schema(&batch.schema(), &self.kv);
            let file = File::create(&self.path)?;
            self.inner = Some(ArrowWriter::try_new(file, schema.clone(), Some(writer_properties()))?);
            self.file_schema = Some(schema);
        }
        // Re-stamp so the batch carries exactly the schema the file declares (as `write_with` does).
        let schema = self.file_schema.clone().unwrap();
        let batch = RecordBatch::try_new(schema, batch.columns().to_vec())?;
        self.inner.as_mut().unwrap().write(&batch)?;
        Ok(())
    }
}

/// Read a timsim artifact, **validating it against the declared schema first**.
///
/// This is the whole point. A stage calls this before it computes anything, so a renamed,
/// missing, or retyped column fails *here* — at the boundary, with a column-level message —
/// rather than three stages downstream inside a `row.get()` that has no idea what went wrong.
pub fn read(path: impl AsRef<Path>, table: &str) -> Result<Vec<RecordBatch>, SchemaError> {
    Ok(read_stream(path, table)?.collect::<Result<Vec<_>, _>>()?)
}

/// Like [`read`], but returns the LAZY row-group iterator instead of materialising every batch. Same
/// stamp/version/conformance checks up front; the data is then pulled a batch at a time, so a consumer
/// can stream an arbitrarily large table in bounded memory (the chunked render depends on this).
pub fn read_stream(
    path: impl AsRef<Path>,
    table: &str,
) -> Result<ParquetRecordBatchReader, SchemaError> {
    let path_s = path.as_ref().display().to_string();
    let spec = tables::by_name(table).ok_or_else(|| {
        SchemaError::UnknownTable(
            table.to_string(),
            tables::all().iter().map(|t| t.name).collect::<Vec<_>>().join(", "),
        )
    })?;

    let file = File::open(&path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let md = schema.metadata();

    // Is this even a timsim artifact? An UNSTAMPED file must not be accepted just because its
    // columns happen to line up — that would let a generic Parquet file, or a legacy artifact from
    // before the schema existed, be wired into a stage without ever proving its identity. The
    // stage-boundary contract is only worth something if it is unconditional.
    let found_table = md.get(meta::TABLE).ok_or_else(|| SchemaError::Unstamped {
        path: path_s.clone(),
        missing: meta::TABLE,
    })?;
    if found_table != table {
        return Err(SchemaError::WrongTable {
            path: path_s,
            found: found_table.clone(),
            expected: table.to_string(),
        });
    }

    // Is it a version we understand?
    let found_version = md.get(meta::VERSION).ok_or_else(|| SchemaError::Unstamped {
        path: path_s.clone(),
        missing: meta::VERSION,
    })?;
    if major(found_version) != major(SCHEMA_VERSION) {
        return Err(SchemaError::IncompatibleVersion {
            path: path_s,
            found: found_version.clone(),
            expected: SCHEMA_VERSION.to_string(),
        });
    }

    // Does it actually have the columns it claims to?
    if let Some(report) = conformance_report(&spec, &schema) {
        return Err(SchemaError::Nonconforming {
            path: path_s,
            table: table.to_string(),
            version: SCHEMA_VERSION.to_string(),
            report,
        });
    }

    Ok(builder.build()?)
}

/// The key-value metadata of an artifact, without reading its data.
pub fn metadata(path: impl AsRef<Path>) -> Result<HashMap<String, String>, SchemaError> {
    let file = File::open(&path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    Ok(builder.schema().metadata().clone())
}

/// Validate a file without reading its data. Powers `timsim schema validate`.
pub fn validate(path: impl AsRef<Path>, table: Option<&str>) -> Result<String, SchemaError> {
    let file = File::open(&path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let md = schema.metadata();

    let table = match table {
        Some(t) => t.to_string(),
        None => md.get(meta::TABLE).cloned().ok_or_else(|| {
            SchemaError::UnknownTable(
                "<none declared in file>".into(),
                tables::all().iter().map(|t| t.name).collect::<Vec<_>>().join(", "),
            )
        })?,
    };
    read(&path, &table)?;
    Ok(format!(
        "ok: {} conforms to {} v{} [{}]",
        path.as_ref().display(),
        table,
        md.get(meta::VERSION).map(|s| s.as_str()).unwrap_or("?"),
        md.get(meta::AXIS).map(|s| s.as_str()).unwrap_or("?"),
    ))
}
