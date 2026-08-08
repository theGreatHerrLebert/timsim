//! **The semantic verification gate.**
//!
//! # The failure this exists to prevent
//!
//! A stage once wrote an 18,021,754-row Parquet file in which one row carried four physically
//! impossible m/z values, the valid values had been shifted into the adjacent rows, and the
//! vacated slots held garbage. The job exited 0. Nothing downstream complained. It was found by
//! chance, months later.
//!
//! # Why the checksums already in the file could not have caught it
//!
//! Parquet pages carry a CRC and ZSTD frames carry a checksum, and **neither is a control over the
//! values**:
//!
//! * The Parquet page CRC is computed over the *serialised page bytes, after compression*. It
//!   attests that the page you read back is the page that was written. It says nothing about
//!   whether the page held the right numbers.
//! * The ZSTD frame checksum hashes the input *as the compressor received it*. If the shifted rows
//!   were already in the buffer handed to the compressor, ZSTD faithfully certifies the garbage.
//!
//! Both are transport checks. They begin *after* the last point at which the corruption could still
//! be seen. Garbage in, certified garbage out — with a green tick.
//!
//! The only place a check has any power is **before the values reach the writer**. So that is where
//! this gate sits: [`digest_batch`] hashes the logical values of each row group on the way in, the
//! hashes are written to a sidecar manifest next to the artifact, and [`verify`] independently
//! re-reads the file, decodes it, recomputes the same hashes, and compares.
//!
//! # What is hashed: the canonical value stream
//!
//! Hashing Arrow's buffers directly would be worthless — the buffer for a given set of values is
//! not unique. Padding, allocation capacity, a slice offset, the width of an offsets array
//! (`Utf8` vs `LargeUtf8`), the presence of an all-valid null bitmap: all of these vary between two
//! arrays that hold *identical data*, and all of them vary between Arrow versions. A hash over
//! buffers would fire on a dependency bump and stay silent on a shifted row.
//!
//! So the gate defines its own serialisation, `timsim-canon/1`, over the **logical values**. It is
//! specified here in full, because a hash whose definition is "whatever the code does" cannot be
//! reproduced by a second implementation, and a gate you cannot reproduce is not a control.
//!
//! ```text
//! column_hash(name, array) = SHA256(
//!       "timsim-canon/1" 0x00 "col" 0x00
//!    ++ name    0x00                       ; UTF-8 column name
//!    ++ typetag 0x00                       ; canonical type class, see below
//!    ++ slots(array, 0, array.len())
//! )
//!
//! slots(a, start, len) =
//!       u64le(len)
//!    ++ u64le(nulls)                       ; nulls within [start, start+len)
//!    ++ if nulls > 0 { len bytes, 0x01 valid / 0x00 null }
//!    ++ value(a, j) for every VALID j in [start, start+len)
//!
//! value(a, j) =
//!    bool          -> 0x00 | 0x01
//!    u8..u64/i8..i64 -> the integer's little-endian bytes (1, 2, 4 or 8)
//!    f16/f32/f64   -> to_bits().to_le_bytes()   ; IEEE-754 bit pattern, little-endian
//!    utf8          -> u64le(byte_len) ++ the UTF-8 bytes
//!    binary        -> u64le(byte_len) ++ the bytes
//!    list          -> slots(child, offset[j], offset[j+1] - offset[j])   ; recursive
//!
//! group_hash = SHA256(
//!       "timsim-canon/1" 0x00 "group" 0x00
//!    ++ u64le(num_rows) ++ u64le(hashed_column_count)
//!    ++ the 32 raw bytes of each column_hash, in column order
//! )
//! ```
//!
//! ## Why this is stable across runs, machines and Arrow versions
//!
//! Every choice above exists to remove a degree of freedom that is *representation*, not *data*:
//!
//! * **Values, never buffers.** Everything is read through the logical accessors, so padding,
//!   capacity, and a non-zero array offset are unreachable by construction.
//! * **Null slots are never hashed.** Arrow leaves whatever it likes in the values buffer under a
//!   null. Hashing it would make the digest depend on uninitialised memory — nondeterministic on
//!   the same machine, let alone across two. Only the validity *pattern* is hashed.
//! * **Endianness is written down.** `to_le_bytes()` everywhere, never a native-order buffer cast,
//!   so a big-endian host produces the same digest.
//! * **Floats are hashed by bit pattern**, so `-0.0` is distinguishable from `0.0` and a NaN
//!   payload change is caught. No normalisation, because normalisation is data loss.
//! * **Offset width is not data.** `Utf8`/`LargeUtf8`/`Utf8View` share the tag `utf8`, and
//!   `List`/`LargeList`/`FixedSizeList` share `list<..>`, because every one of those pairs holds
//!   the same values and Arrow picks between them for its own reasons. This mirrors
//!   [`crate::tables`]' existing rule that a list's *inner field name* (`item` vs `element`) is not
//!   part of the contract.
//! * **Lengths are always framed.** Each variable-length value carries its own `u64le` length, so
//!   `["ab", "c"]` and `["a", "bc"]` cannot collide.
//! * **The hash is SHA-256**, a standard with a fixed output. Upgrading the `sha2` crate cannot
//!   change a digest. The canon has its own version string (`timsim-canon/1`) so that if the
//!   *serialisation* ever must change, old sidecars announce which rules they were written under
//!   instead of silently comparing unequal.
//!
//! The one thing deliberately *not* collapsed is integer and float **width**: `u32` and `u64` get
//! different tags. A width change is a schema change, it is caught on read by
//! [`crate::read`] anyway, and an integrity gate should not be the component that decides two
//! different schemas are the same thing.
//!
//! ## Columns the canon cannot express
//!
//! A stage is allowed to annotate an artifact with extra columns (see `extra_columns_are_permitted`
//! in the conformance tests), and one could carry a type this canon has no encoding for — a
//! `Struct`, a `Timestamp`, a `Dictionary`. Two behaviours were possible and only one is honest:
//! **refusing the write** would turn the gate into a regression for producers that do nothing
//! wrong, while **silently skipping the column** would be exactly the class of quiet omission this
//! module exists to prevent. So such a column is skipped *and named*, in the manifest's
//! `unhashed_columns` and in every [`Report`], and [`verify`] fails if the two sides disagree about
//! which columns those are. Coverage is stated, never assumed. Every type in [`crate::tables`] is
//! covered, so in practice the list is empty.

use crate::{SchemaError, TableSpec, ROW_GROUP_ROWS, SCHEMA_VERSION};
use arrow::array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray,
    Float16Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, StringArray, StringViewArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{ArrowPrimitiveType, DataType, Float16Type};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::path::{Path, PathBuf};

/// The canonical-serialisation version. Recorded in every sidecar; a sidecar written under a
/// different canon is reported as unverifiable rather than compared and declared corrupt.
pub const CANON: &str = "timsim-canon/1";

/// The sidecar document format version.
pub const MANIFEST_FORMAT: &str = "timsim-integrity/1";

/// Appended to the artifact's **full file name**, so `precursors.parquet` is accompanied by
/// `precursors.parquet.integrity.json`. Appended rather than substituted for the extension so that
/// two artifacts differing only in extension cannot share — and silently overwrite — one manifest.
pub const SIDECAR_SUFFIX: &str = ".integrity.json";

/// Where the manifest for `artifact` lives.
pub fn sidecar_path(artifact: impl AsRef<Path>) -> PathBuf {
    let p = artifact.as_ref();
    let name = p.file_name().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
    p.with_file_name(format!("{name}{SIDECAR_SUFFIX}"))
}

// ─────────────────────────────────────────────────────────────────────────────
// The canonical type tag
// ─────────────────────────────────────────────────────────────────────────────

/// The canonical type class of `dt`, or `None` if `timsim-canon/1` has no encoding for it.
///
/// Representation-only distinctions are collapsed (see the module docs); width and signedness are
/// not.
pub fn type_tag(dt: &DataType) -> Option<String> {
    let s = match dt {
        DataType::Boolean => "bool".to_string(),
        DataType::UInt8 => "u8".to_string(),
        DataType::UInt16 => "u16".to_string(),
        DataType::UInt32 => "u32".to_string(),
        DataType::UInt64 => "u64".to_string(),
        DataType::Int8 => "i8".to_string(),
        DataType::Int16 => "i16".to_string(),
        DataType::Int32 => "i32".to_string(),
        DataType::Int64 => "i64".to_string(),
        DataType::Float16 => "f16".to_string(),
        DataType::Float32 => "f32".to_string(),
        DataType::Float64 => "f64".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "utf8".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "binary".to_string(),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            format!("list<{}>", type_tag(f.data_type())?)
        }
        _ => return None,
    };
    Some(s)
}

// ─────────────────────────────────────────────────────────────────────────────
// The hash sink
// ─────────────────────────────────────────────────────────────────────────────

/// Feeding SHA-256 eight bytes at a time is dominated by call overhead, so the canonical stream is
/// staged through a buffer and handed over in blocks. The bytes hashed are identical either way —
/// this is purely about not paying a function call per scalar.
const FLUSH_AT: usize = 64 * 1024;

struct Sink {
    hasher: Sha256,
    buf: Vec<u8>,
}

impl Sink {
    fn new(kind: &str) -> Self {
        let mut s = Sink { hasher: Sha256::new(), buf: Vec::with_capacity(FLUSH_AT + 4096) };
        s.put(CANON.as_bytes());
        s.put(&[0]);
        s.put(kind.as_bytes());
        s.put(&[0]);
        s
    }

    #[inline]
    fn put(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
        if self.buf.len() >= FLUSH_AT {
            self.hasher.update(&self.buf);
            self.buf.clear();
        }
    }

    #[inline]
    fn put_u64(&mut self, v: u64) {
        self.put(&v.to_le_bytes());
    }

    fn finish(mut self) -> [u8; 32] {
        self.hasher.update(&self.buf);
        self.hasher.finalize().into()
    }
}

fn hex(bytes: &[u8]) -> String {
    const D: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(D[(b >> 4) as usize] as char);
        s.push(D[(b & 0xf) as usize] as char);
    }
    s
}

// ─────────────────────────────────────────────────────────────────────────────
// The canonical encoder
// ─────────────────────────────────────────────────────────────────────────────

/// Nulls in `[start, start+len)`.
///
/// Computed over the **range**, never over the whole array: a `ListArray` keeps its full child when
/// sliced, so "does this array have nulls" answered globally would give a row group a different
/// answer on the write side (where the child spans the whole table) than on the read side (where it
/// spans one row group). That asymmetry would have made every digest of a nullable-child list
/// column mismatch for no reason.
fn nulls_in(array: &dyn Array, start: usize, len: usize) -> usize {
    match array.nulls() {
        None => 0,
        Some(nb) => {
            if start == 0 && len == array.len() {
                nb.null_count()
            } else {
                len - nb.inner().slice(start, len).count_set_bits()
            }
        }
    }
}

/// `slots(a, start, len)` from the module docs.
fn encode_slots(array: &dyn Array, start: usize, len: usize, sink: &mut Sink) -> Option<()> {
    sink.put_u64(len as u64);
    let nulls = nulls_in(array, start, len);
    sink.put_u64(nulls as u64);
    if nulls > 0 {
        for j in start..start + len {
            sink.put(&[array.is_valid(j) as u8]);
        }
    }
    encode_values(array, start, len, nulls > 0, sink)
}

macro_rules! numeric {
    ($array:expr, $ty:ty, $start:expr, $len:expr, $sparse:expr, $sink:expr, $conv:expr) => {{
        let a = $array.as_any().downcast_ref::<$ty>()?;
        let v = a.values();
        let conv = $conv;
        if $sparse {
            for j in $start..$start + $len {
                if a.is_valid(j) {
                    $sink.put(&conv(v[j]));
                }
            }
        } else {
            for j in $start..$start + $len {
                $sink.put(&conv(v[j]));
            }
        }
    }};
}

/// Every variable-length value is framed with its own `u64le` length, so `["ab", "c"]` and
/// `["a", "bc"]` cannot produce the same stream.
fn put_varlen<'a>(sink: &mut Sink, items: impl Iterator<Item = &'a [u8]>) {
    for b in items {
        sink.put_u64(b.len() as u64);
        sink.put(b);
    }
}

/// `$get` is written as a method chain on the bound array `a` and index `j`, not as a closure: a
/// closure returning a borrow of its own argument needs a higher-ranked bound that inference will
/// not supply here, and spelling that out would be far more noise than the substitution.
macro_rules! varlen {
    ($array:expr, $ty:ty, $start:expr, $len:expr, $sparse:expr, $sink:expr, $a:ident, $j:ident, $get:expr) => {{
        let $a = $array.as_any().downcast_ref::<$ty>()?;
        put_varlen(
            $sink,
            ($start..$start + $len).filter(|j| !($sparse && $a.is_null(*j))).map(|$j| $get),
        );
    }};
}

/// `value(a, j)` for every valid `j` in the range. `sparse` says whether nulls are present at all,
/// so the null-free case — which is nearly every column in [`crate::tables`] — skips the per-slot
/// validity lookup entirely.
fn encode_values(
    array: &dyn Array,
    start: usize,
    len: usize,
    sparse: bool,
    sink: &mut Sink,
) -> Option<()> {
    match array.data_type() {
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>()?;
            for j in start..start + len {
                if sparse && a.is_null(j) {
                    continue;
                }
                sink.put(&[a.value(j) as u8]);
            }
        }

        DataType::UInt8 => numeric!(array, UInt8Array, start, len, sparse, sink, u8::to_le_bytes),
        DataType::UInt16 => numeric!(array, UInt16Array, start, len, sparse, sink, u16::to_le_bytes),
        DataType::UInt32 => numeric!(array, UInt32Array, start, len, sparse, sink, u32::to_le_bytes),
        DataType::UInt64 => numeric!(array, UInt64Array, start, len, sparse, sink, u64::to_le_bytes),
        DataType::Int8 => numeric!(array, Int8Array, start, len, sparse, sink, i8::to_le_bytes),
        DataType::Int16 => numeric!(array, Int16Array, start, len, sparse, sink, i16::to_le_bytes),
        DataType::Int32 => numeric!(array, Int32Array, start, len, sparse, sink, i32::to_le_bytes),
        DataType::Int64 => numeric!(array, Int64Array, start, len, sparse, sink, i64::to_le_bytes),

        // Bit pattern, not value: `-0.0` must not hash as `0.0`, and a NaN payload change is a
        // change to the file.
        // Named through the Arrow type rather than `half::f16` so this does not pin a transitive
        // crate that `timsim-schema` does not depend on directly.
        DataType::Float16 => {
            numeric!(array, Float16Array, start, len, sparse, sink, |v: <Float16Type as ArrowPrimitiveType>::Native| v
                .to_bits()
                .to_le_bytes())
        }
        DataType::Float32 => {
            numeric!(array, Float32Array, start, len, sparse, sink, |v: f32| v
                .to_bits()
                .to_le_bytes())
        }
        DataType::Float64 => {
            numeric!(array, Float64Array, start, len, sparse, sink, |v: f64| v
                .to_bits()
                .to_le_bytes())
        }

        DataType::Utf8 => {
            varlen!(array, StringArray, start, len, sparse, sink, a, j, a.value(j).as_bytes())
        }
        DataType::LargeUtf8 => {
            varlen!(array, LargeStringArray, start, len, sparse, sink, a, j, a.value(j).as_bytes())
        }
        DataType::Utf8View => {
            varlen!(array, StringViewArray, start, len, sparse, sink, a, j, a.value(j).as_bytes())
        }
        DataType::Binary => {
            varlen!(array, BinaryArray, start, len, sparse, sink, a, j, a.value(j))
        }
        DataType::LargeBinary => {
            varlen!(array, LargeBinaryArray, start, len, sparse, sink, a, j, a.value(j))
        }
        DataType::BinaryView => {
            varlen!(array, BinaryViewArray, start, len, sparse, sink, a, j, a.value(j))
        }
        DataType::FixedSizeBinary(_) => {
            varlen!(array, FixedSizeBinaryArray, start, len, sparse, sink, a, j, a.value(j))
        }

        DataType::List(_) => {
            let a = array.as_any().downcast_ref::<ListArray>()?;
            let off = a.value_offsets();
            let child = a.values().as_ref();
            for j in start..start + len {
                if sparse && a.is_null(j) {
                    continue;
                }
                let (lo, hi) = (off[j] as usize, off[j + 1] as usize);
                encode_slots(child, lo, hi - lo, sink)?;
            }
        }
        DataType::LargeList(_) => {
            let a = array.as_any().downcast_ref::<LargeListArray>()?;
            let off = a.value_offsets();
            let child = a.values().as_ref();
            for j in start..start + len {
                if sparse && a.is_null(j) {
                    continue;
                }
                let (lo, hi) = (off[j] as usize, off[j + 1] as usize);
                encode_slots(child, lo, hi - lo, sink)?;
            }
        }
        DataType::FixedSizeList(_, n) => {
            let a = array.as_any().downcast_ref::<FixedSizeListArray>()?;
            let n = *n as usize;
            let child = a.values().as_ref();
            for j in start..start + len {
                if sparse && a.is_null(j) {
                    continue;
                }
                encode_slots(child, j * n, n, sink)?;
            }
        }

        // Unreachable for any column whose `type_tag` was `Some`, which is the only kind that
        // reaches here. Kept as a hard stop rather than a silent success.
        _ => return None,
    }
    Some(())
}

fn column_digest(name: &str, array: &dyn Array) -> Option<[u8; 32]> {
    let tag = type_tag(array.data_type())?;
    let mut sink = Sink::new("col");
    sink.put(name.as_bytes());
    sink.put(&[0]);
    sink.put(tag.as_bytes());
    sink.put(&[0]);
    encode_slots(array, 0, array.len(), &mut sink)?;
    Some(sink.finish())
}

/// The canonical hash of one column, as lower-case hex.
///
/// `None` when the column's type has no `timsim-canon/1` encoding — the caller records it as
/// unhashed rather than pretending it was covered.
pub fn column_hash(name: &str, array: &dyn Array) -> Option<String> {
    column_digest(name, array).map(|d| hex(&d))
}

// ─────────────────────────────────────────────────────────────────────────────
// Digests
// ─────────────────────────────────────────────────────────────────────────────

/// A column and the canonical type class it was hashed under.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnRef {
    pub name: String,
    #[serde(rename = "type")]
    pub type_tag: String,
}

/// One column's digest within a row group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDigest {
    pub name: String,
    pub hash: String,
}

/// One row group's digest.
///
/// `hash` is derived from `columns`, so a verifier that finds a group mismatch can name the
/// offending column without re-reading anything, and a manifest is internally checkable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowGroupDigest {
    pub index: usize,
    pub num_rows: u64,
    pub hash: String,
    pub columns: Vec<ColumnDigest>,
}

/// Digest one row group's worth of rows.
pub fn digest_row_group(batch: &RecordBatch, index: usize) -> RowGroupDigest {
    let mut columns = Vec::with_capacity(batch.num_columns());
    let mut group = Sink::new("group");
    group.put_u64(batch.num_rows() as u64);

    // Two passes so the column count is known before the per-column hashes are folded in — a
    // length-prefixed list cannot be extended by appending, which is the point.
    let mut raw: Vec<[u8; 32]> = Vec::with_capacity(batch.num_columns());
    for (i, field) in batch.schema().fields().iter().enumerate() {
        if let Some(d) = column_digest(field.name(), batch.column(i).as_ref()) {
            columns.push(ColumnDigest { name: field.name().clone(), hash: hex(&d) });
            raw.push(d);
        }
    }
    group.put_u64(raw.len() as u64);
    for b in &raw {
        group.put(b);
    }

    RowGroupDigest {
        index,
        num_rows: batch.num_rows() as u64,
        hash: hex(&group.finish()),
        columns,
    }
}

/// Which columns the canon covers and which it does not — carried together so the two halves of
/// the answer cannot drift apart, and so "verified" always comes with its extent attached.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Coverage {
    /// Covered columns, in schema order, with the type class each was hashed under.
    pub hashed: Vec<ColumnRef>,
    /// Columns `timsim-canon/1` has no encoding for, named with their Arrow type. Empty for every
    /// table in [`crate::tables`].
    pub unhashed: Vec<ColumnRef>,
}

impl Coverage {
    pub fn of(batch: &RecordBatch) -> Coverage {
        Coverage::of_schema(&batch.schema())
    }

    /// Coverage implied by a schema alone — used for an artifact that never received a row, whose
    /// manifest must still state what it would have covered.
    pub fn of_schema(schema: &arrow::datatypes::Schema) -> Coverage {
        let mut c = Coverage::default();
        for f in schema.fields() {
            match type_tag(f.data_type()) {
                Some(t) => c.hashed.push(ColumnRef { name: f.name().clone(), type_tag: t }),
                None => c.unhashed.push(ColumnRef {
                    name: f.name().clone(),
                    type_tag: f.data_type().to_string(),
                }),
            }
        }
        c
    }
}

/// Digest a whole batch, split at exactly the boundaries the Parquet writer will use.
///
/// [`crate::write`] hands the Arrow writer one batch and the writer slices it into
/// [`ROW_GROUP_ROWS`]-row row groups; the digests have to line up with those groups or the manifest
/// describes a partition of the data that the file does not have. The split is asserted against the
/// writer's own reported layout after the file is closed (see `check_layout`), so a future Arrow
/// that groups differently fails loudly instead of emitting a manifest that never verifies.
pub fn digest_batch(batch: &RecordBatch, start_index: usize) -> Vec<RowGroupDigest> {
    let mut out = Vec::new();
    let mut off = 0usize;
    let mut idx = start_index;
    while off < batch.num_rows() {
        let n = ROW_GROUP_ROWS.min(batch.num_rows() - off);
        out.push(digest_row_group(&batch.slice(off, n), idx));
        off += n;
        idx += 1;
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// The sidecar manifest
// ─────────────────────────────────────────────────────────────────────────────

/// The sidecar written next to an artifact: what the values *were*, at the moment before the
/// writer touched them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Document format — [`MANIFEST_FORMAT`].
    pub format: String,
    /// Canonical serialisation the hashes were computed under — [`CANON`].
    pub canon: String,
    /// Hash function. Present so the document says what it is rather than requiring the reader to
    /// know; a differing value is reported as unverifiable.
    pub hash: String,
    pub schema_version: String,
    pub table: String,
    pub axis: String,
    pub producer: String,
    /// The artifact this manifest describes, file name only — a sidecar that has been moved next to
    /// a *different* parquet file is caught rather than silently applied to it.
    pub parquet_file: String,
    pub total_rows: u64,
    /// Every covered column, in schema order, with the type class it was hashed under.
    pub columns: Vec<ColumnRef>,
    /// Columns `timsim-canon/1` has no encoding for. Empty for every table in [`crate::tables`].
    /// Stated explicitly so that "verified" always means a known extent of coverage.
    pub unhashed_columns: Vec<ColumnRef>,
    pub row_groups: Vec<RowGroupDigest>,
}

impl Manifest {
    /// Read a manifest. `Ok(None)` when the file does not exist — an artifact written before this
    /// gate existed is *unverifiable*, which is a different claim from *corrupt*.
    pub fn load(path: impl AsRef<Path>) -> Result<Option<Manifest>, SchemaError> {
        let p = path.as_ref();
        match std::fs::read(p) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
            Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|e| {
                SchemaError::Manifest { path: p.display().to_string(), detail: e.to_string() }
            }),
        }
    }

    /// Write the manifest, pretty-printed and newline-terminated.
    ///
    /// The document holds no timestamp and no host name, deliberately: re-running a deterministic
    /// stage produces a byte-identical sidecar, so a sidecar can be diffed and content-addressed
    /// exactly like the artifact it describes.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<(), SchemaError> {
        let mut s = serde_json::to_vec_pretty(self).map_err(|e| SchemaError::Manifest {
            path: path.as_ref().display().to_string(),
            detail: e.to_string(),
        })?;
        s.push(b'\n');
        std::fs::write(path, s)?;
        Ok(())
    }
}

/// Assemble and write the sidecar for a freshly written artifact.
pub(crate) fn write_manifest(
    path: &Path,
    spec: &TableSpec,
    table: &str,
    producer: &str,
    total_rows: u64,
    coverage: Coverage,
    row_groups: Vec<RowGroupDigest>,
) -> Result<(), SchemaError> {
    let m = Manifest {
        format: MANIFEST_FORMAT.to_string(),
        canon: CANON.to_string(),
        hash: "sha256".to_string(),
        schema_version: SCHEMA_VERSION.to_string(),
        table: table.to_string(),
        axis: spec.axis.as_str().to_string(),
        producer: producer.to_string(),
        parquet_file: path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default(),
        total_rows,
        columns: coverage.hashed,
        unhashed_columns: coverage.unhashed,
        row_groups,
    };
    m.save(sidecar_path(path))
}

/// Cross-check the row-group partition the gate hashed against the one the writer actually laid
/// down, using the writer's own returned metadata.
///
/// The gate predicts the split (at [`ROW_GROUP_ROWS`]) because the hashes must be computed *before*
/// the values are handed over, and by the time the true layout is known the values are gone. A
/// prediction that is merely *usually* right would produce manifests that fail verification for a
/// reason having nothing to do with the data — so it is checked, once, against ground truth.
pub(crate) fn check_layout(
    path: &Path,
    predicted: &[RowGroupDigest],
    actual: &parquet::format::FileMetaData,
) -> Result<(), SchemaError> {
    let found: Vec<u64> = actual.row_groups.iter().map(|rg| rg.num_rows as u64).collect();
    let expect: Vec<u64> = predicted.iter().map(|g| g.num_rows).collect();
    if found != expect {
        return Err(SchemaError::RowGroupLayout {
            path: path.display().to_string(),
            expected: expect,
            found,
        });
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Verification
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Every row group's recomputed hash equals the recorded one.
    Verified,
    /// No sidecar, or a sidecar written under rules this build cannot apply. **Not** a corruption
    /// finding: artifacts predating the gate have no manifest and must not be reported as bad.
    Unverifiable,
    /// The decoded values do not reproduce the recorded hashes.
    Mismatch,
}

/// The first divergence found, located as precisely as the manifest allows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mismatch {
    pub row_group: usize,
    /// Named when the manifest carries per-column digests and exactly which column moved can be
    /// determined; `None` when only the group hash differs (e.g. the row count itself changed).
    pub column: Option<String>,
    pub expected: String,
    pub found: String,
    pub detail: String,
}

/// The outcome of verifying one artifact.
#[derive(Clone, Debug)]
pub struct Report {
    pub path: PathBuf,
    pub sidecar: Option<PathBuf>,
    pub status: Status,
    pub table: Option<String>,
    pub rows: u64,
    pub row_groups: usize,
    /// Columns actually covered by the recomputed hashes.
    pub columns_hashed: usize,
    /// Columns the canon could not express — coverage stated, not assumed.
    pub columns_unhashed: Vec<String>,
    pub first_mismatch: Option<Mismatch>,
    /// One line, for a human.
    pub summary: String,
}

impl Report {
    pub fn is_ok(&self) -> bool {
        self.status != Status::Mismatch
    }
}

fn unverifiable(path: &Path, sidecar: Option<PathBuf>, reason: &str) -> Report {
    Report {
        path: path.to_path_buf(),
        sidecar,
        status: Status::Unverifiable,
        table: None,
        rows: 0,
        row_groups: 0,
        columns_hashed: 0,
        columns_unhashed: Vec::new(),
        first_mismatch: None,
        summary: format!("unverifiable: {} — {reason}", path.display()),
    }
}

/// **Independently** re-read `path`, recompute the canonical hashes from the decoded values, and
/// compare them to the sidecar.
///
/// Independent is the operative word: nothing is reused from the write. The file is reopened, the
/// pages are decompressed and decoded through the ordinary reader, and the canon is applied to the
/// values that come out — so this catches a writer that mis-serialised as readily as a disk that
/// rotted, and it is the same code path a consumer three stages downstream would take.
///
/// A missing sidecar yields [`Status::Unverifiable`], never [`Status::Mismatch`].
pub fn verify(path: impl AsRef<Path>) -> Result<Report, SchemaError> {
    let path = path.as_ref();
    let side = sidecar_path(path);

    let manifest = match Manifest::load(&side)? {
        None => return Ok(unverifiable(path, None, "no integrity sidecar next to this artifact")),
        Some(m) => m,
    };
    if manifest.canon != CANON {
        return Ok(unverifiable(
            path,
            Some(side),
            &format!("sidecar was written under {} but this build speaks {CANON}", manifest.canon),
        ));
    }
    if manifest.hash != "sha256" {
        return Ok(unverifiable(
            path,
            Some(side),
            &format!("sidecar uses hash {:?}, which this build cannot compute", manifest.hash),
        ));
    }

    let actual_name = path.file_name().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
    if manifest.parquet_file != actual_name {
        return Ok(unverifiable(
            path,
            Some(side),
            &format!(
                "sidecar describes {:?}, not {:?} — a manifest has been moved next to the wrong file",
                manifest.parquet_file, actual_name
            ),
        ));
    }

    let mut report = Report {
        path: path.to_path_buf(),
        sidecar: Some(side),
        status: Status::Verified,
        table: Some(manifest.table.clone()),
        rows: 0,
        row_groups: 0,
        columns_hashed: manifest.columns.len(),
        columns_unhashed: manifest.unhashed_columns.iter().map(|c| c.name.clone()).collect(),
        first_mismatch: None,
        summary: String::new(),
    };

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let file_meta = builder.metadata().clone();
    let n_groups = file_meta.num_row_groups();

    // The partition itself is part of the claim: if the file has a different number of row groups,
    // or a group of a different length, the manifest does not describe this file and no amount of
    // matching hashes would make it do so.
    if n_groups != manifest.row_groups.len() {
        report.status = Status::Mismatch;
        report.first_mismatch = Some(Mismatch {
            row_group: 0,
            column: None,
            expected: format!("{} row groups", manifest.row_groups.len()),
            found: format!("{n_groups} row groups"),
            detail: "the file's row-group partition differs from the one recorded at write time"
                .to_string(),
        });
        report.summary = format!(
            "MISMATCH: {} — recorded {} row groups, file has {n_groups}",
            path.display(),
            manifest.row_groups.len()
        );
        return Ok(report);
    }

    for (i, want) in manifest.row_groups.iter().enumerate() {
        let rg_rows = file_meta.row_group(i).num_rows() as u64;
        if rg_rows != want.num_rows {
            report.status = Status::Mismatch;
            report.first_mismatch = Some(Mismatch {
                row_group: i,
                column: None,
                expected: format!("{} rows", want.num_rows),
                found: format!("{rg_rows} rows"),
                detail: "row group length differs from the one recorded at write time".to_string(),
            });
            report.summary = format!(
                "MISMATCH: {} — row group {i} holds {rg_rows} rows, {} were recorded",
                path.display(),
                want.num_rows
            );
            return Ok(report);
        }

        // One row group at a time: bounded memory, and the same decode path a consumer uses.
        let f = File::open(path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)?
            .with_row_groups(vec![i])
            .with_batch_size(rg_rows.max(1) as usize)
            .build()?;
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()));
        let batch = arrow::compute::concat_batches(&schema, &batches)?;

        let got = digest_row_group(&batch, i);
        report.rows += got.num_rows;
        report.row_groups += 1;

        if got.hash != want.hash {
            // The group hash is a hash *of the column hashes*, so the column that moved is already
            // in hand — no second pass, no guessing.
            let column = want
                .columns
                .iter()
                .find(|c| !got.columns.iter().any(|g| g.name == c.name && g.hash == c.hash))
                .map(|c| c.name.clone())
                .or_else(|| {
                    got.columns
                        .iter()
                        .find(|g| !want.columns.iter().any(|c| c.name == g.name))
                        .map(|g| g.name.clone())
                });
            let (expected, found) = match &column {
                Some(name) => (
                    want.columns
                        .iter()
                        .find(|c| &c.name == name)
                        .map(|c| c.hash.clone())
                        .unwrap_or_else(|| "<absent>".into()),
                    got.columns
                        .iter()
                        .find(|c| &c.name == name)
                        .map(|c| c.hash.clone())
                        .unwrap_or_else(|| "<absent>".into()),
                ),
                None => (want.hash.clone(), got.hash.clone()),
            };
            report.status = Status::Mismatch;
            report.summary = match &column {
                Some(name) => format!(
                    "MISMATCH: {} — row group {i} ({} rows), column {name:?}: recorded {}, decoded {}",
                    path.display(),
                    want.num_rows,
                    &expected[..expected.len().min(16)],
                    &found[..found.len().min(16)],
                ),
                None => format!(
                    "MISMATCH: {} — row group {i} ({} rows) does not reproduce its recorded hash",
                    path.display(),
                    want.num_rows
                ),
            };
            report.first_mismatch = Some(Mismatch {
                row_group: i,
                column,
                expected,
                found,
                detail: "the decoded values do not reproduce the hash taken before the writer saw \
                         them"
                    .to_string(),
            });
            return Ok(report);
        }

        // Coverage must agree too: a column that was hashed at write time and is unhashable now
        // (or the reverse) means the two sides are not looking at the same thing.
        let now_unhashed: Vec<String> =
            Coverage::of(&batch).unhashed.into_iter().map(|c| c.name).collect();
        if now_unhashed != report.columns_unhashed {
            report.status = Status::Mismatch;
            report.first_mismatch = Some(Mismatch {
                row_group: i,
                column: None,
                expected: format!("uncovered columns {:?}", report.columns_unhashed),
                found: format!("uncovered columns {now_unhashed:?}"),
                detail: "the set of columns the canon can express changed between write and read"
                    .to_string(),
            });
            report.summary =
                format!("MISMATCH: {} — hash coverage differs from the manifest", path.display());
            return Ok(report);
        }
    }

    if report.rows != manifest.total_rows {
        report.status = Status::Mismatch;
        report.first_mismatch = Some(Mismatch {
            row_group: 0,
            column: None,
            expected: format!("{} rows", manifest.total_rows),
            found: format!("{} rows", report.rows),
            detail: "total row count differs from the manifest".to_string(),
        });
        report.summary = format!(
            "MISMATCH: {} — {} rows recorded, {} decoded",
            path.display(),
            manifest.total_rows,
            report.rows
        );
        return Ok(report);
    }

    let coverage = if report.columns_unhashed.is_empty() {
        format!("all {} columns", report.columns_hashed)
    } else {
        format!(
            "{} of {} columns ({} not covered by {CANON}: {})",
            report.columns_hashed,
            report.columns_hashed + report.columns_unhashed.len(),
            report.columns_unhashed.len(),
            report.columns_unhashed.join(", ")
        )
    };
    report.summary = format!(
        "verified: {} — {} rows in {} row group{}, {coverage}, table {} v{} [{}]",
        path.display(),
        report.rows,
        report.row_groups,
        if report.row_groups == 1 { "" } else { "s" },
        manifest.table,
        manifest.schema_version,
        manifest.axis,
    );
    Ok(report)
}
