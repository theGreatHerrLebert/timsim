# Plan — per-protein regulation: honour the request, and let magnitudes vary

> **STATUS: IMPLEMENTED** (timsim-chem 0.2.0 / timsim-schema 0.2.0 / timsim-cli).
> Gaps 1, 2 and 3 are closed, and every unit assertion in the Validation section below is a test
> in `timsim-chem/src/design.rs`. Summary of what shipped:
>
> * `Regulation::Explicit { proteins: BTreeMap<String, f64> }` — per-protein magnitudes;
>   `Condition.regulate` is a `Vec<Regulation>`. TOML: a list of `kind`-tagged blocks; the scalar
>   `{ proteins = [...], log2fc = … }` still parses and warns. Duplicate accessions across blocks
>   and two generative blocks in one condition are errors; explicit beats generative, order-free.
> * Explicit accessions are **forced present** (union over all conditions, so both arms of a
>   contrast have them) and displace the lowest-ranked non-regulated proteins, so the count stays
>   exactly `n_proteins`. Doesn't fit ⇒ error naming the minimum that does.
> * `protein_quantities` gains `requested_log2fc` (authored); `true_log2fc` keeps its realised
>   meaning. Schema 2.0 → 2.1 (additive).
> * `timsim-yield` counts surviving peptides per regulated protein, warns on zero, and writes a
>   `[regulated_peptides]` table into `--report`. The digest sample stays uniform.
> * Bonus, found while verifying "identical output": the per-sample load rescale summed a
>   `HashMap`, whose order is seeded per process — two runs of one spec differed in the last bits.
>   Now summed in Vec order, and asserted bit-identical.
>
> One deviation, argued in the test itself: *seed stability* is asserted as **membership unchanged
> + a single common rescale within the affected organism + no change at all elsewhere**, not as
> unchanged absolute `amount_amol`. The load is fixed, so swapping one protein for another must
> move mass between them; what must not change — and does not — is any *relative* abundance.

Two defects in `timsim-chem`'s design axis, found by running a real PhantomBENCH cohort through v2
(`design.rs`; spec deserialisation in `timsim-cli/src/spec.rs`). Both concern `[[condition]].regulate`,
which is otherwise wired end-to-end and already emits `true_log2fc` per condition as the answer key.

## Gap 2 (correctness, the important one) — regulation is silently dropped

**What happens.** `regulate` is validated against the **proteome** (`design.rs:~585` — a stale accession is a
hard error, deliberately, "so a typo in it must not silently produce a design with no regulation at all").
But it is *applied* only to proteins that are **present in the sample**, and presence is decided
independently by `[design].n_proteins` via a seeded identity-keyed rank that knows nothing about regulation.

Measured: PhantomBENCH's 14 regulated accessions at `n_proteins = 2400` → **10 silently dropped**
(`is_regulated = false`, amount 0), no warning. You ask for 14, get 4, and the run looks clean.

**Why it matters.** This is the same guarantee the accession validator already promises, leaking one stage
later. The failure is invisible and produces a *confidently wrong* benchmark: a DE analysis over that cohort
under-recovers the planted signature and the shortfall is indistinguishable from poor tool performance —
precisely the "silently stale/wrong artifact" class this project exists to kill.

**Proposed behaviour: regulated ⇒ present.** A protein named in `Regulation::Explicit` is forced into the
present set regardless of `n_proteins`, because naming it *is* a statement that the experiment is about it.
`n_proteins` then fills the remaining slots as today.

**Resolved (claudex):**
1. **Force + DISPLACE, not expand.** A bench scientist reads `n_proteins = 2400` as sample complexity, not
   a floor. Force the named proteins in, then fill the remaining slots with the highest-ranked *non-forced*
   proteins, so the final count is exactly `n_proteins`. (My initial preference for expanding was wrong.)
   If the regulated set alone exceeds `n_proteins`, **error** with the minimum `n_proteins` that fits.
2. **Force, don't refuse**, in the normal case — regulation is an intentional intervention and silently
   omitting it is unacceptable. Refusal is reserved for "cannot fit", above.
3. **`Generative` is unaffected** and must NOT force anything: it is defined over the *final* present set.
   Explicit forcing happens BEFORE generative selection; document that order. Overlap between an explicit
   and a generative rule on the same protein must be a deterministic, tested rule (error, or defined
   precedence) — not an accident of iteration order.
4. **Use the protein's natural identity-keyed abundance rank.** Critically, that rank must be independent
   of *membership* selection, so forcing a protein in does not reshuffle abundances for unrelated proteins.
   This is a reproducibility requirement, and is asserted in Validation below.

## Gap 1 (fidelity) — one magnitude per condition

`Regulation::Explicit { proteins: Vec<String>, log2fc: f64 }` carries **one** log2fc for the whole set, and
`regulate = [{...}, {...}]` is rejected (verified: *"data did not match any variant of untagged enum
RegulateSpec"*). PhantomBENCH authors a 0.5–1.8 spread across 14 proteins; v2 collapses it to a uniform 1.0.

**Why it matters.** A volcano over the planted set then ranks by *noise* rather than effect size — the
protein that should top it does so only by chance. Any benchmark that scores ranking, or that reports
"recovered the strongest effectors", is measuring something different from what v1 measured.

**Resolved (claudex): a LIST of TAGGED blocks, where an explicit block takes a per-protein map.** Neither
of my two candidates alone was right — this gets composition *and* per-protein magnitudes:

```toml
regulate = [
  { kind = "explicit", proteins = { P0DJI8 = 1.6, P02741 = 1.4, P17540 = 0.7 } },
  { kind = "generative", fraction = 0.05, log2fc_sd = 1.0 },
]
```

The old scalar form (`{ proteins = [...], log2fc = 1.0 }`) stays as a **deprecated compatibility**
deserialisation path; only the new canonical form is documented and emitted. Duplicate explicit accessions
across blocks must be rejected (or given an explicit, tested precedence) rather than resolved by ordering.

The stale-accession validation must extend to **every** entry in every block.

## Gap 3 — NOT documentation-only after all (claudex correction)

`--max-peptides` samples the digest uniformly, so at 3 000 only 1 of 14 regulated proteins survives (they
hold 0.06 % of peptides). The *sampling* is correct and must stay unbiased — making it regulation-aware
would bias the digest, which is worse. **But treating this as a docs line was wrong**: a user can otherwise
score a DE analysis against an effect that is *physically unobservable*, silently. That is the same failure
class as gap 2, one stage further down.

**Required:** report answer-key **observability** — warn (or error under a strict flag) when a regulated
protein has zero surviving peptides, and record per-regulated-protein peptide counts so a cohort can be
judged scoreable before it is searched. Documentation (cap off or ≥ a few hundred thousand for a DE cohort)
remains, but is no longer the whole fix.

## Mass balance and the answer key (claudex — the catch that would have bitten)

Applying fold changes and then renormalising to the declared load **changes the realised log2FC of every
protein**, including unregulated ones. We already measured this: a declared 1.0 realised as +0.94. With
per-protein magnitudes that distortion varies per protein, so the two quantities must be kept separate:

- **`true_log2fc`** = the **realised** between-condition ratio, computed from final amounts after all
  scaling and compositional normalisation. This is the answer key, and it is what a DE analysis can
  actually recover. (Today's column already has this meaning — it must keep it.)
- **`requested_log2fc`** = the authored intervention, recorded alongside so a run can be traced back to its
  spec. New column.

Also required:
- **Baseline presence:** a forced protein must be present in **both** arms of any contrast, or its
  `true_log2fc` is undefined/infinite. Forcing must therefore apply per-condition consistently.
- **Collision rules:** duplicate accessions across blocks, conflicting signs, and explicit-vs-generative
  overlap need deterministic, tested validation — not iteration order.

## Validation (strengthened — the original would have passed a wrong implementation)

Unit:
- A regulated accession excluded by `n_proteins` ends up present and regulated (gap 2).
- The final present count is **exactly `n_proteins`** — proves displace, not expand.
- Non-forced membership equals the prior ranked selection **minus the deterministically displaced** entries
  — proves forcing didn't reshuffle unrelated proteins.
- **Seed stability:** adding a forced protein leaves every unrelated protein's abundance and membership
  unchanged. Asserted directly, not inferred.
- Total protein amount still reconciles with the declared **load** after regulation (mass balance holds).
- `true_log2fc` equals the **realised** per-protein quantity ratio (not the requested value), and
  `requested_log2fc` carries the authored one.
- A stale accession still errors; duplicate/conflicting entries error.
- The regulated set exceeding `n_proteins` errors, naming the minimum that fits.

End-to-end (PhantomBENCH two-arm Bruker cohort):
- **14/14** regulated present (was 4/14), background ~0.00 (sd ~0.21).
- Measured per-protein log2FC tracks the *authored* 0.5–1.8 spread — explicitly NOT a uniform +0.94, which
  is what a magnitude-collapsing implementation would produce.
- Every regulated protein has ≥1 surviving peptide, or is reported unobservable (gap 3).

Backwards compatibility: every existing design TOML in `timsim-necro`'s `flow/configs/` still parses and
produces identical output; the deprecated scalar `regulate` form still works. None currently uses
`regulate`, so the risk is a serde regression rather than a behaviour change.
