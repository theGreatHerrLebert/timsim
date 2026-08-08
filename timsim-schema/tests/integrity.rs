//! Tests for the semantic verification gate.
//!
//! The gate exists because of one specific incident: an 18,021,754-row artifact in which four m/z
//! values in a single row were physically impossible, the real values had been shifted into the
//! adjacent rows, and the vacated slots held garbage — written, checksummed by Parquet and ZSTD,
//! and exited 0. So the first thing tested here is that exact shape of corruption
//! (`a_row_shift_with_garbage_in_the_vacated_slots_is_caught`), and the rest establishes that the
//! hash is *specific* enough to be worth trusting and *stable* enough not to cry wolf.

use arrow::array::{
    ArrayRef, Date32Array, Float32Array, Float64Array, Int32Array, LargeStringArray,
    ListArray, StringArray, UInt16Array, UInt64Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use timsim_schema::integrity::{self, Status};
use timsim_schema::{read, write, Writer};

fn dir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

/// `peptides`, the smallest table with a string, an integer and a float in it.
fn peptides(n: usize, mass_of: impl Fn(usize) -> f64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("peptide_id", DataType::UInt64, false),
        Field::new("sequence", DataType::Utf8, false),
        Field::new("length", DataType::UInt16, false),
        Field::new("mass_monoisotopic", DataType::Float64, false),
    ]));
    let seqs: Vec<String> = (0..n).map(|i| format!("PEPTIDEK{i}")).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from((0..n as u64).collect::<Vec<_>>())),
            Arc::new(StringArray::from(seqs)),
            Arc::new(UInt16Array::from((0..n).map(|i| 8 + (i % 3) as u16).collect::<Vec<_>>())),
            Arc::new(Float64Array::from((0..n).map(&mass_of).collect::<Vec<_>>())),
        ],
    )
    .unwrap()
}

fn hash_of(batch: &RecordBatch) -> String {
    integrity::digest_row_group(batch, 0).hash
}

// ─────────────────────────────────────────────────────────────────────────────
// The incident
// ─────────────────────────────────────────────────────────────────────────────

/// **The failure that motivated all of this.** One row's values are physically impossible, the
/// values that belonged there have moved into the neighbouring rows, and the vacated slots hold
/// whatever was lying around. Every column still has the right name, the right type and the right
/// row count; the file is perfectly well-formed. Only the values are wrong.
#[test]
fn a_row_shift_with_garbage_in_the_vacated_slots_is_caught() {
    let good = peptides(64, |i| 1000.0 + i as f64);
    let before = hash_of(&good);

    let mut masses: Vec<f64> = (0..64).map(|i| 1000.0 + i as f64).collect();
    // Shift four values one slot down, and leave garbage where they came from.
    for k in (30..34).rev() {
        masses[k + 1] = masses[k];
    }
    for (k, junk) in (30..34).zip([1.7e308, -4.9e-324, 0.0, 6.02e23]) {
        masses[k] = junk;
    }
    let corrupt = peptides(64, |i| masses[i]);

    assert_ne!(
        before,
        hash_of(&corrupt),
        "a shifted row with garbage in the vacated slots produced the same canonical hash — the \
         gate would not have caught the incident it exists for"
    );
}

/// The *last bit* of one value, in one row, out of a million — the smallest change a `f64` column
/// can undergo. Written as a bit flip rather than an arithmetic nudge because at this magnitude
/// `x + f64::EPSILON` rounds straight back to `x`, which would have made this test vacuous.
#[test]
fn one_flipped_bit_in_a_million_rows_changes_the_hash() {
    let n = 1_000_000;
    let a = peptides(n, |i| 1000.0 + i as f64);
    let b = peptides(n, |i| {
        let v = 1000.0 + i as f64;
        if i == 617_283 {
            f64::from_bits(v.to_bits() ^ 1)
        } else {
            v
        }
    });
    assert_ne!(hash_of(&a), hash_of(&b));
}

/// Values in the right multiset but the wrong order — the shape a mis-sorted join produces.
#[test]
fn a_permutation_is_not_the_same_artifact() {
    let a = peptides(16, |i| 1000.0 + i as f64);
    let b = peptides(16, |i| 1000.0 + (if i == 3 { 4 } else if i == 4 { 3 } else { i }) as f64);
    assert_ne!(hash_of(&a), hash_of(&b));
}

// ─────────────────────────────────────────────────────────────────────────────
// Round trip
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn an_artifact_verifies_against_the_sidecar_written_beside_it() {
    let d = dir();
    let p = d.path().join("peptides.parquet");
    write(&p, "peptides", &peptides(5000, |i| 1000.0 + i as f64), "test/1.0", None).unwrap();

    assert!(integrity::sidecar_path(&p).exists(), "no sidecar was written");
    let r = timsim_schema::verify(&p).unwrap();
    assert_eq!(r.status, Status::Verified, "{}", r.summary);
    assert_eq!(r.rows, 5000);
    assert_eq!(r.row_groups, 1);
    assert_eq!(r.columns_hashed, 4);
    assert!(r.columns_unhashed.is_empty());
}

#[test]
fn the_manifest_records_the_row_group_partition_the_file_actually_has() {
    let d = dir();
    let p = d.path().join("peptides.parquet");
    // Deliberately more than one row group.
    let n = timsim_schema::ROW_GROUP_ROWS + 7;
    write(&p, "peptides", &peptides(n, |i| i as f64), "test/1.0", None).unwrap();

    let m = integrity::Manifest::load(integrity::sidecar_path(&p)).unwrap().unwrap();
    assert_eq!(m.total_rows, n as u64);
    assert_eq!(m.row_groups.len(), 2);
    assert_eq!(m.row_groups[0].num_rows, timsim_schema::ROW_GROUP_ROWS as u64);
    assert_eq!(m.row_groups[1].num_rows, 7);
    assert_eq!(m.row_groups[0].index, 0);
    assert_eq!(m.row_groups[1].index, 1);
    assert_eq!(m.canon, integrity::CANON);
    assert_eq!(m.format, integrity::MANIFEST_FORMAT);

    assert_eq!(timsim_schema::verify(&p).unwrap().status, Status::Verified);
}

/// The streaming writer already produces byte-identical Parquet; its manifest must agree too, or
/// the same table would carry two different integrity claims depending on how it was produced.
#[test]
fn the_streamed_manifest_matches_the_single_batch_one() {
    let d = dir();
    let n = timsim_schema::ROW_GROUP_ROWS + 1234;
    let all = peptides(n, |i| 1000.5 + i as f64);

    let one = d.path().join("one.parquet");
    write(&one, "peptides", &all, "test/1.0", None).unwrap();

    let many = d.path().join("many.parquet");
    let mut w = Writer::new(&many, "peptides", "test/1.0", None).unwrap();
    let mut off = 0;
    for chunk in [7, 900_000, 60_000, n - 7 - 900_000 - 60_000] {
        w.write(&all.slice(off, chunk)).unwrap();
        off += chunk;
    }
    w.close().unwrap();

    assert_eq!(std::fs::read(&one).unwrap(), std::fs::read(&many).unwrap(), "parquet drifted");

    let a = integrity::Manifest::load(integrity::sidecar_path(&one)).unwrap().unwrap();
    let b = integrity::Manifest::load(integrity::sidecar_path(&many)).unwrap().unwrap();
    assert_eq!(a.row_groups, b.row_groups);
    assert_eq!(a.columns, b.columns);
    assert_eq!(a.total_rows, b.total_rows);

    assert_eq!(timsim_schema::verify(&many).unwrap().status, Status::Verified);
}

#[test]
fn an_empty_artifact_gets_a_manifest_that_says_so() {
    let d = dir();
    let p = d.path().join("empty.parquet");
    Writer::new(&p, "peptides", "test/1.0", None).unwrap().close().unwrap();

    let m = integrity::Manifest::load(integrity::sidecar_path(&p)).unwrap().unwrap();
    assert_eq!(m.total_rows, 0);
    assert!(m.row_groups.is_empty());
    // An empty artifact is a *verifiable claim of emptiness*, not an absence of one.
    assert_eq!(timsim_schema::verify(&p).unwrap().status, Status::Verified);
}

// ─────────────────────────────────────────────────────────────────────────────
// Detection through a real file
// ─────────────────────────────────────────────────────────────────────────────

/// Corrupt the bytes on disk, inside the first data page, and confirm the artifact no longer
/// verifies. Either outcome counts as caught: the decoder may refuse the page outright, or it may
/// hand back values that do not reproduce the recorded hash.
#[test]
fn a_flipped_byte_in_the_written_file_fails_verification() {
    let d = dir();
    let p = d.path().join("peptides.parquet");
    write(&p, "peptides", &peptides(20_000, |i| 1000.0 + i as f64), "test/1.0", None).unwrap();
    assert_eq!(timsim_schema::verify(&p).unwrap().status, Status::Verified);

    // Aim at real payload rather than a magic number or padding: the first column chunk's data
    // page, a little way in, so the flip lands in the page body.
    let f = std::fs::File::open(&p).unwrap();
    let md = parquet::file::reader::SerializedFileReader::new(f).unwrap();
    let off = {
        use parquet::file::reader::FileReader;
        md.metadata().row_group(0).column(0).data_page_offset() as usize
    };

    let mut bytes = std::fs::read(&p).unwrap();
    let target = off + 32;
    bytes[target] ^= 0xff;
    std::fs::write(&p, &bytes).unwrap();

    let outcome = timsim_schema::verify(&p);
    match outcome {
        Ok(r) => assert_eq!(
            r.status,
            Status::Mismatch,
            "a flipped byte inside a data page still verified: {}",
            r.summary
        ),
        Err(_) => { /* the decoder refused the page — also caught */ }
    }
}

/// A corruption that a decoder cannot notice at all: the values decode perfectly, they are simply
/// the wrong ones. This is the case in-format checksums are structurally blind to.
#[test]
fn verify_names_the_row_group_and_the_column_that_moved() {
    let d = dir();
    let p = d.path().join("peptides.parquet");
    let n = timsim_schema::ROW_GROUP_ROWS + 500;
    write(&p, "peptides", &peptides(n, |i| 1000.0 + i as f64), "test/1.0", None).unwrap();

    // Rewrite the artifact with one value changed in the SECOND row group, then move the original
    // (correct) manifest back beside it — exactly the situation where the file is internally
    // consistent and only the gate can tell.
    let good_manifest = std::fs::read(integrity::sidecar_path(&p)).unwrap();
    write(
        &p,
        "peptides",
        &peptides(n, |i| if i == timsim_schema::ROW_GROUP_ROWS + 100 { -1.0 } else { 1000.0 + i as f64 }),
        "test/1.0",
        None,
    )
    .unwrap();
    std::fs::write(integrity::sidecar_path(&p), &good_manifest).unwrap();

    let r = timsim_schema::verify(&p).unwrap();
    assert_eq!(r.status, Status::Mismatch, "{}", r.summary);
    let m = r.first_mismatch.expect("no mismatch recorded");
    assert_eq!(m.row_group, 1, "wrong row group named");
    assert_eq!(m.column.as_deref(), Some("mass_monoisotopic"), "wrong column named");
    assert_ne!(m.expected, m.found);
}

// ─────────────────────────────────────────────────────────────────────────────
// Absent and mismatched sidecars
// ─────────────────────────────────────────────────────────────────────────────

/// Artifacts written before the gate existed have no manifest. Reporting those as corrupt would
/// drown the real signal, so the answer is "unverifiable" — a different claim, and an honest one.
#[test]
fn an_artifact_without_a_sidecar_is_unverifiable_not_corrupt() {
    let d = dir();
    let p = d.path().join("peptides.parquet");
    write(&p, "peptides", &peptides(10, |i| i as f64), "test/1.0", None).unwrap();
    std::fs::remove_file(integrity::sidecar_path(&p)).unwrap();

    let r = timsim_schema::verify(&p).unwrap();
    assert_eq!(r.status, Status::Unverifiable);
    assert!(r.is_ok(), "an unverifiable artifact must not be reported as a failure");
    assert!(r.summary.contains("no integrity sidecar"), "{}", r.summary);
}

/// A manifest that has been copied next to a different artifact must not be applied to it — that
/// would let one file's good hashes vouch for another file's contents.
#[test]
fn a_manifest_beside_the_wrong_artifact_is_refused() {
    let d = dir();
    let a = d.path().join("a.parquet");
    let b = d.path().join("b.parquet");
    write(&a, "peptides", &peptides(10, |i| i as f64), "test/1.0", None).unwrap();
    write(&b, "peptides", &peptides(10, |i| i as f64), "test/1.0", None).unwrap();

    std::fs::copy(integrity::sidecar_path(&a), integrity::sidecar_path(&b)).unwrap();
    let r = timsim_schema::verify(&b).unwrap();
    assert_eq!(r.status, Status::Unverifiable);
    assert!(r.summary.contains("moved next to the wrong file"), "{}", r.summary);
}

#[test]
fn a_manifest_from_a_future_canon_is_unverifiable_rather_than_wrong() {
    let d = dir();
    let p = d.path().join("peptides.parquet");
    write(&p, "peptides", &peptides(10, |i| i as f64), "test/1.0", None).unwrap();

    let side = integrity::sidecar_path(&p);
    let mut m = integrity::Manifest::load(&side).unwrap().unwrap();
    m.canon = "timsim-canon/99".to_string();
    m.save(&side).unwrap();

    let r = timsim_schema::verify(&p).unwrap();
    assert_eq!(r.status, Status::Unverifiable);
    assert!(r.summary.contains("timsim-canon/99"), "{}", r.summary);
}

#[test]
fn an_unreadable_sidecar_is_an_error_not_a_silent_pass() {
    let d = dir();
    let p = d.path().join("peptides.parquet");
    write(&p, "peptides", &peptides(10, |i| i as f64), "test/1.0", None).unwrap();
    std::fs::write(integrity::sidecar_path(&p), b"{ this is not json").unwrap();
    assert!(timsim_schema::verify(&p).is_err());
}

// ─────────────────────────────────────────────────────────────────────────────
// Stability: representation must not change the hash
// ─────────────────────────────────────────────────────────────────────────────

/// The offset width of a string array is Arrow's business, not the data's. If it leaked into the
/// hash, a producer switching to `LargeUtf8` — or an Arrow version choosing differently on read —
/// would report every artifact as corrupt.
#[test]
fn the_offset_width_of_a_string_column_is_not_data() {
    let small: ArrayRef = Arc::new(StringArray::from(vec!["PEPTIDE", "KKK", ""]));
    let large: ArrayRef = Arc::new(LargeStringArray::from(vec!["PEPTIDE", "KKK", ""]));
    assert_eq!(
        integrity::column_hash("sequence", small.as_ref()),
        integrity::column_hash("sequence", large.as_ref())
    );
}

/// Same for a slice offset: a sliced array and a freshly built one holding the same values are the
/// same data, however differently they are laid out in memory.
#[test]
fn a_slice_offset_is_not_data() {
    let whole = Float64Array::from(vec![9.0, 9.0, 1.5, 2.5, 3.5, 9.0]);
    let sliced = whole.slice(2, 3);
    let fresh = Float64Array::from(vec![1.5, 2.5, 3.5]);
    assert_eq!(integrity::column_hash("mz", &sliced), integrity::column_hash("mz", &fresh));
}

/// A `List` and a `LargeList` over the same peaks are the same peaks.
#[test]
fn the_offset_width_of_a_list_column_is_not_data() {
    fn list(field_name: &str) -> ListArray {
        let values = Float32Array::from(vec![1.0f32, 2.0, 3.0, 4.0]);
        ListArray::new(
            Arc::new(Field::new(field_name, DataType::Float32, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 4])),
            Arc::new(values),
            None,
        )
    }
    // Arrow names a list's element "item"; the Parquet spec says "element", and pyarrow writes
    // that. `crate::types_compatible` already treats the two as the same; so must the hash.
    assert_eq!(
        integrity::column_hash("isotope_intensity", &list("item")),
        integrity::column_hash("isotope_intensity", &list("element"))
    );
}

/// Arrow leaves whatever it likes in the values buffer underneath a null. Hashing it would make the
/// digest depend on uninitialised memory — nondeterministic on one machine, let alone across two.
#[test]
fn the_bytes_underneath_a_null_are_never_hashed() {
    let validity = NullBuffer::from(vec![true, false, true, false, true]);
    let a = Float64Array::new(
        ScalarBuffer::from(vec![1.0, 12345.6789, 3.0, -0.0, 5.0]),
        Some(validity.clone()),
    );
    let b = Float64Array::new(
        // Same valid values; completely different junk under the nulls.
        ScalarBuffer::from(vec![1.0, f64::NAN, 3.0, f64::INFINITY, 5.0]),
        Some(validity),
    );
    assert_eq!(integrity::column_hash("x", &a), integrity::column_hash("x", &b));
}

#[test]
fn a_null_is_not_a_zero() {
    let with_null =
        Float64Array::new(ScalarBuffer::from(vec![1.0, 0.0]), Some(NullBuffer::from(vec![true, false])));
    let with_zero = Float64Array::from(vec![1.0, 0.0]);
    assert_ne!(integrity::column_hash("x", &with_null), integrity::column_hash("x", &with_zero));
}

/// `-0.0 == 0.0` in arithmetic, but they are different eight-byte values in the file, and an
/// integrity control that normalises is an integrity control with a blind spot.
#[test]
fn negative_zero_is_not_zero() {
    assert_ne!(
        integrity::column_hash("x", &Float64Array::from(vec![-0.0f64])),
        integrity::column_hash("x", &Float64Array::from(vec![0.0f64]))
    );
}

/// Length-framing: reflowing the same bytes across different strings must not collide.
#[test]
fn strings_cannot_be_reflowed_unnoticed() {
    assert_ne!(
        integrity::column_hash("s", &StringArray::from(vec!["ab", "c"])),
        integrity::column_hash("s", &StringArray::from(vec!["a", "bc"]))
    );
}

/// Same for lists — the incident's own shape, one level down: the peaks are all still there, they
/// have merely moved between rows.
#[test]
fn peaks_cannot_move_between_rows_unnoticed() {
    fn list(offsets: Vec<i32>) -> ListArray {
        ListArray::new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Arc::new(Float32Array::from(vec![1.0f32, 2.0, 3.0, 4.0])),
            None,
        )
    }
    assert_ne!(
        integrity::column_hash("isotope_intensity", &list(vec![0, 2, 4])),
        integrity::column_hash("isotope_intensity", &list(vec![0, 1, 4]))
    );
}

/// Width and signedness stay in the tag: `u32` and `i32` hold the same bytes and mean different
/// numbers, and the gate is not the component that gets to decide they are interchangeable.
#[test]
fn signedness_is_part_of_the_type_tag() {
    assert_eq!(integrity::type_tag(&DataType::UInt32).unwrap(), "u32");
    assert_eq!(integrity::type_tag(&DataType::Int32).unwrap(), "i32");
    assert_ne!(
        integrity::column_hash("n", &UInt64Array::from(vec![1u64, 2])),
        integrity::column_hash("n", &Int32Array::from(vec![1i32, 2]))
    );
}

/// Renaming a column changes the artifact — the `retention_time_gru_predictor` lesson, applied to
/// values as well as to schemas.
#[test]
fn the_column_name_is_part_of_the_hash() {
    let a = Float64Array::from(vec![1.0, 2.0]);
    assert_ne!(integrity::column_hash("mz", &a), integrity::column_hash("mass", &a));
}

/// Pinned digests of a fixed input.
///
/// Two jobs. First, the guard that a refactor, an Arrow upgrade or a dependency bump has not
/// quietly changed what `timsim-canon/1` *means*: if these values move, every sidecar ever written
/// has become unverifiable, and that has to be a deliberate act — a new canon version — rather than
/// a side effect nobody noticed.
///
/// Second, evidence that the canon is genuinely *specified* and not merely "whatever the code
/// does". These constants were reproduced by an independent twenty-line implementation written from
/// the prose in [`timsim_schema::integrity`] alone, in another language, with no reference to this
/// crate. A hash a second implementation cannot reproduce is not a contract.
///
/// The input is `peptides` with three rows:
/// `peptide_id = [0, 1, 2]`, `sequence = ["PEPTIDEK0", "PEPTIDEK1", "PEPTIDEK2"]`,
/// `length = [8, 9, 10]`, `mass_monoisotopic = [1000.5, 1001.5, 1002.5]`.
#[test]
fn the_canonical_hash_is_pinned() {
    let batch = peptides(3, |i| 1000.5 + i as f64);
    let expect = [
        ("peptide_id", "c9bd8be2eb2cd484e07f9d6fc93b752d83a1aff074782afced06b4294ad5559c"),
        ("sequence", "fc742ab1154e110079a3e0fc386ee6391577312b2781e2e9dd48754d6804025c"),
        ("length", "b9f4189567e3de55f16421a1f1031070837f985fa42e41434e9172d1fcd4eeb8"),
        ("mass_monoisotopic", "d47a1e52d18d52e8e703cfdbe667485644e9fdfb6f47327c332bb5f71090ef56"),
    ];
    for (i, (name, want)) in expect.iter().enumerate() {
        assert_eq!(
            integrity::column_hash(name, batch.column(i).as_ref()).as_deref(),
            Some(*want),
            "column {name} no longer hashes to its pinned value — timsim-canon/1 has changed meaning"
        );
    }
    assert_eq!(
        hash_of(&batch),
        "ac2fbb6af0255f9cc886d3c98bec68ce7b9c276dfa91be473207a3277b6bddc0",
        "the row-group hash no longer folds the column hashes the way it is documented to"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Coverage is stated, never assumed
// ─────────────────────────────────────────────────────────────────────────────

/// A stage may annotate an artifact with an extra column, and one could carry a type the canon has
/// no encoding for. Refusing the write would punish a producer doing nothing wrong; skipping the
/// column quietly would be the very failure mode this module exists to prevent. So it is skipped
/// **and named**, in the manifest and in the report.
#[test]
fn a_column_the_canon_cannot_express_is_named_not_ignored() {
    assert!(integrity::type_tag(&DataType::Date32).is_none());

    let d = dir();
    let p = d.path().join("peptides.parquet");
    let base = peptides(100, |i| i as f64);
    let mut fields: Vec<Field> = base.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new("harvested_on", DataType::Date32, false));
    let mut cols: Vec<ArrayRef> = base.columns().to_vec();
    cols.push(Arc::new(Date32Array::from((0..100).collect::<Vec<i32>>())));
    let annotated = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap();

    write(&p, "peptides", &annotated, "test/1.0", None).unwrap();

    let m = integrity::Manifest::load(integrity::sidecar_path(&p)).unwrap().unwrap();
    assert_eq!(m.columns.len(), 4);
    assert_eq!(m.unhashed_columns.len(), 1);
    assert_eq!(m.unhashed_columns[0].name, "harvested_on");

    let r = timsim_schema::verify(&p).unwrap();
    assert_eq!(r.status, Status::Verified, "{}", r.summary);
    assert_eq!(r.columns_unhashed, vec!["harvested_on".to_string()]);
    // "verified" must never be readable as "all of it was checked" when it was not.
    assert!(r.summary.contains("not covered"), "{}", r.summary);
    assert!(r.summary.contains("harvested_on"), "{}", r.summary);
}

// ─────────────────────────────────────────────────────────────────────────────
// The CLI
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn the_cli_reports_and_exits_accordingly() {
    let exe = env!("CARGO_BIN_EXE_timsim-verify");
    let d = dir();
    let p = d.path().join("peptides.parquet");
    write(&p, "peptides", &peptides(1000, |i| i as f64), "test/1.0", None).unwrap();

    let ok = std::process::Command::new(exe).arg(&p).output().unwrap();
    assert_eq!(ok.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&ok.stdout).contains("verified:"));

    // No sidecar: exit 0 by default, exit 1 under --require-sidecar.
    std::fs::remove_file(integrity::sidecar_path(&p)).unwrap();
    let bare = std::process::Command::new(exe).arg(&p).output().unwrap();
    assert_eq!(bare.status.code(), Some(0));
    let strict =
        std::process::Command::new(exe).arg("--require-sidecar").arg(&p).output().unwrap();
    assert_eq!(strict.status.code(), Some(1));

    // A real mismatch: exit 1, and machine-readable on request.
    let good = peptides(1000, |i| i as f64);
    write(&p, "peptides", &good, "test/1.0", None).unwrap();
    let manifest = std::fs::read(integrity::sidecar_path(&p)).unwrap();
    write(&p, "peptides", &peptides(1000, |i| if i == 500 { -7.0 } else { i as f64 }), "test/1.0", None)
        .unwrap();
    std::fs::write(integrity::sidecar_path(&p), manifest).unwrap();

    let bad = std::process::Command::new(exe).arg("--json").arg(&p).output().unwrap();
    assert_eq!(bad.status.code(), Some(1));
    let out = String::from_utf8_lossy(&bad.stdout);
    assert!(out.contains("\"status\":\"mismatch\""), "{out}");
    assert!(out.contains("mass_monoisotopic"), "{out}");
}

/// The artifact must still be readable by the ordinary reader with the gate in place — the sidecar
/// is beside the file, never inside it.
#[test]
fn the_sidecar_does_not_disturb_the_reader() {
    let d = dir();
    let p = d.path().join("peptides.parquet");
    let batch = peptides(1234, |i| 1000.0 + i as f64);
    write(&p, "peptides", &batch, "test/1.0", None).unwrap();

    let back = read(&p, "peptides").unwrap();
    let rows: usize = back.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1234);
}
