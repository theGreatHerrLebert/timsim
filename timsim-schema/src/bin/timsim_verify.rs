//! `timsim-verify` — the independent re-read half of the integrity gate.
//!
//! Re-reads each artifact, decodes it, recomputes the `timsim-canon/1` hashes from the decoded
//! values, and compares them to the `<file>.integrity.json` sidecar written when the artifact was
//! produced. See [`timsim_schema::integrity`] for what is hashed and why the checksums already
//! inside a Parquet file cannot do this job.
//!
//! ```text
//! timsim-verify [options] <artifact.parquet>...
//!
//!   --require-sidecar   treat a missing sidecar as a failure (default: "unverifiable", exit 0)
//!   --json              one JSON object per artifact on stdout
//!   --quiet             print only failures
//!   -h, --help
//!
//! exit 0  every artifact verified (or was unverifiable, without --require-sidecar)
//! exit 1  at least one artifact did not reproduce its recorded hashes
//! exit 2  usage error, or an artifact could not be read at all
//! ```
//!
//! This is a **new** binary. No existing binary's flags are touched.

use std::process::ExitCode;
use timsim_schema::integrity::{self, Status};

const USAGE: &str = "\
timsim-verify — recompute an artifact's canonical value hashes and compare them to its sidecar

USAGE:
    timsim-verify [OPTIONS] <artifact.parquet>...

OPTIONS:
    --require-sidecar   an artifact with no integrity sidecar is a failure, not merely
                        unverifiable. Off by default: artifacts written before the gate
                        existed legitimately have none, and calling those \"corrupt\" would
                        make the tool's output worthless.
    --json              emit one JSON object per artifact instead of prose
    --quiet             print only failures
    -h, --help          this text

EXIT CODES:
    0   all verified (unverifiable counts as ok unless --require-sidecar)
    1   at least one artifact did not reproduce its recorded hashes
    2   usage error, or an artifact could not be read
";

fn esc(s: &str) -> String {
    s.chars()
        .flat_map(|c| match c {
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\\' => "\\\\".chars().collect(),
            '\n' => "\\n".chars().collect(),
            '\t' => "\\t".chars().collect(),
            c if (c as u32) < 0x20 => format!("\\u{:04x}", c as u32).chars().collect(),
            c => vec![c],
        })
        .collect()
}

fn main() -> ExitCode {
    let mut require_sidecar = false;
    let mut json = false;
    let mut quiet = false;
    let mut paths: Vec<String> = Vec::new();

    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--require-sidecar" => require_sidecar = true,
            "--json" => json = true,
            "--quiet" => quiet = true,
            "-h" | "--help" => {
                print!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            a if a.starts_with('-') => {
                eprintln!("timsim-verify: unknown option {a:?}\n\n{USAGE}");
                return ExitCode::from(2);
            }
            a => paths.push(a.to_string()),
        }
    }

    if paths.is_empty() {
        eprintln!("timsim-verify: no artifact given\n\n{USAGE}");
        return ExitCode::from(2);
    }

    let mut worst = 0u8;
    for p in &paths {
        let report = match integrity::verify(p) {
            Ok(r) => r,
            Err(e) => {
                // A file that cannot even be decoded is a failure, not an unverifiable: a corrupt
                // page that makes ZSTD refuse to decompress arrives here.
                if json {
                    println!(
                        "{{\"path\":\"{}\",\"status\":\"error\",\"detail\":\"{}\"}}",
                        esc(p),
                        esc(&e.to_string())
                    );
                } else {
                    eprintln!("ERROR: {p}: {e}");
                }
                worst = worst.max(2);
                continue;
            }
        };

        let failed = match report.status {
            Status::Mismatch => true,
            Status::Unverifiable => require_sidecar,
            Status::Verified => false,
        };
        if failed {
            worst = worst.max(1);
        }

        if json {
            let status = match report.status {
                Status::Verified => "verified",
                Status::Unverifiable => "unverifiable",
                Status::Mismatch => "mismatch",
            };
            let mut s = format!(
                "{{\"path\":\"{}\",\"status\":\"{status}\",\"rows\":{},\"row_groups\":{},\
                 \"columns_hashed\":{},\"columns_unhashed\":[{}],\"summary\":\"{}\"",
                esc(&report.path.display().to_string()),
                report.rows,
                report.row_groups,
                report.columns_hashed,
                report
                    .columns_unhashed
                    .iter()
                    .map(|c| format!("\"{}\"", esc(c)))
                    .collect::<Vec<_>>()
                    .join(","),
                esc(&report.summary),
            );
            if let Some(m) = &report.first_mismatch {
                s.push_str(&format!(
                    ",\"first_mismatch\":{{\"row_group\":{},\"column\":{},\"expected\":\"{}\",\
                     \"found\":\"{}\",\"detail\":\"{}\"}}",
                    m.row_group,
                    match &m.column {
                        Some(c) => format!("\"{}\"", esc(c)),
                        None => "null".to_string(),
                    },
                    esc(&m.expected),
                    esc(&m.found),
                    esc(&m.detail),
                ));
            }
            s.push('}');
            println!("{s}");
        } else if failed {
            eprintln!("{}", report.summary);
            if let Some(m) = &report.first_mismatch {
                eprintln!("  row group {}", m.row_group);
                if let Some(c) = &m.column {
                    eprintln!("  column     {c:?}");
                }
                eprintln!("  recorded   {}", m.expected);
                eprintln!("  decoded    {}", m.found);
                eprintln!("  {}", m.detail);
            }
        } else if !quiet {
            println!("{}", report.summary);
        }
    }

    ExitCode::from(worst)
}
