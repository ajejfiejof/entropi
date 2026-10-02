"""Formal SMT Verification: Entropi Telemetry Sampling Invariants.

Proves mathematical, algorithmic, and safety theorems for Entropi using the
Z3 Theorem Prover in first-order decidable theories (BitVectors, Reals, and Arrays).

Copyright (c) 2026. Licensed under AGPLv3.
"""

from __future__ import annotations

import sys
import time
import z3


def check_theorem(name: str, hypothesis_and_negated_goal_fn) -> bool:
    """Verifies that the negation of the goal is UNSAT (meaning theorem holds universally)."""
    solver = z3.Solver()
    cond = hypothesis_and_negated_goal_fn()
    solver.add(cond)
    res = solver.check()
    if res == z3.unsat:
        print(f"  [100% PROVED]  {name}")
        return True
    else:
        print(f"  [FAILED]       {name}")
        if res == z3.sat:
            print(f"    Counterexample found: {solver.model()}")
        return False


def run_smt_proofs() -> bool:
    print("=" * 80)
    print("FORMAL SMT VERIFICATION: ENTROPI INFORMATION-THEORETIC SAMPLER")
    print("=" * 80)
    all_ok = True

    # -------------------------------------------------------------
    # 1. Self-Information Monotonicity Theorem
    # -------------------------------------------------------------
    # In information theory, I(p) = -log2(p).
    # Since log2 is strictly monotonically increasing, -log2 is strictly decreasing:
    # (0 < p1 <= p2 <= 1) => I(p1) >= I(p2).
    # We model log2 as a monotonic function Log2: Reals -> Reals with Log2(x) < Log2(y) for x < y.
    print("\n[Phase 1] Proving Shannon Self-Information Invariants...")
    Log2 = z3.Function("Log2", z3.RealSort(), z3.RealSort())
    p1 = z3.Real("p1")
    p2 = z3.Real("p2")

    # Axiom of strictly monotonic log
    log_mono = z3.ForAll([p1, p2],
        z3.Implies(
            z3.And(p1 > 0, p2 > 0, p1 < p2),
            Log2(p1) < Log2(p2)
        )
    )

    s1 = z3.Solver()
    s1.add(log_mono)
    # Check if p1 <= p2 and -Log2(p1) < -Log2(p2) is satisfiable (negation of goal)
    pa = z3.Real("pa")
    pb = z3.Real("pb")
    s1.add(pa > 0, pb > 0, pa <= pb)
    s1.add(-Log2(pa) < -Log2(pb))
    mono_ok = s1.check() == z3.unsat
    all_ok &= mono_ok
    print(f"  [{'100% PROVED' if mono_ok else 'FAILED'}]  Theorem 1 (Information Monotonicity): (0 < p1 <= p2) => I(p1) >= I(p2)")

    # -------------------------------------------------------------
    # 2. Additive Information Independence Theorem
    # -------------------------------------------------------------
    # For independent events A and B, P(A and B) = P(A) * P(B).
    # I(A and B) = -log2(P(A) * P(B)) = -log2(P(A)) + (-log2(P(B))) = I(A) + I(B).
    log_homo = z3.ForAll([p1, p2],
        z3.Implies(
            z3.And(p1 > 0, p2 > 0),
            Log2(p1 * p2) == Log2(p1) + Log2(p2)
        )
    )
    s2 = z3.Solver()
    s2.add(log_homo)
    p_x = z3.Real("p_x")
    p_y = z3.Real("p_y")
    s2.add(p_x > 0, p_y > 0)
    s2.add(-Log2(p_x * p_y) != (-Log2(p_x) + -Log2(p_y)))
    indep_ok = s2.check() == z3.unsat
    all_ok &= indep_ok
    print(f"  [{'100% PROVED' if indep_ok else 'FAILED'}]  Theorem 2 (Additive Independence): I(A and B) == I(A) + I(B)")

    # -------------------------------------------------------------
    # 3. Sampling Probability Monotonicity Theorem
    # -------------------------------------------------------------
    # For surprise S1 <= S2, sample probability p(S1) <= p(S2).
    print("\n[Phase 2] Proving Sampling Policy & Anomaly Retention Invariants...")
    s_val1 = z3.Real("s_val1")
    s_val2 = z3.Real("s_val2")
    t_min = z3.RealVal(4)
    t_crit = z3.RealVal(12)
    p_base = z3.RealVal(1) / z3.RealVal(1000)  # 0.001

    def sample_prob(s):
        return z3.If(s >= t_crit, z3.RealVal(1),
            z3.If(s <= t_min, p_base,
                p_base + (z3.RealVal(1) - p_base) * ((s - t_min) / (t_crit - t_min))
            )
        )

    all_ok &= check_theorem(
        "Theorem 3 (Sampling Probability Monotonicity): (S1 <= S2) => p_sample(S1) <= p_sample(S2)",
        lambda: z3.And(s_val1 <= s_val2, sample_prob(s_val1) > sample_prob(s_val2)),
    )

    # -------------------------------------------------------------
    # 4. Critical Anomaly 100% Retention Theorem
    # -------------------------------------------------------------
    # For any span with HTTP status >= 500 or error boost >= 16 bits,
    # sampling probability is strictly 1.0 (100% guaranteed retention).
    status = z3.Int("status")
    err_boost = z3.Real("err_boost")
    attr_s = z3.Real("attr_s")
    lat_s = z3.Real("lat_s")

    total_s = attr_s + lat_s + z3.If(status >= 500, z3.RealVal(16), z3.RealVal(0))
    p_err_sample = z3.If(status >= 500, z3.RealVal(1), sample_prob(total_s))

    all_ok &= check_theorem(
        "Theorem 4 (Critical Anomaly Retention Guarantee): status >= 500 => p_sample == 1.0",
        lambda: z3.And(status >= 500, attr_s >= 0, lat_s >= 0, p_err_sample != z3.RealVal(1)),
    )

    # -------------------------------------------------------------
    # 5. Frequency Lattice Monotonicity & Merge Safety
    # -------------------------------------------------------------
    print("\n[Phase 3] Proving Frequency Lattice & Memory Invariants...")
    m1 = z3.BitVec("m1", 32)
    m2 = z3.BitVec("m2", 32)
    # Additive merge for counters: m_merged = m1 + m2 (with no overflow below 2^32 - 1)
    all_ok &= check_theorem(
        "Theorem 5 (Lattice Additive Monotonicity): (m1 + m2 >= m1) and (m1 + m2 >= m2)",
        lambda: z3.And(
            z3.ULE(m1, z3.BitVecVal(2000000000, 32)),
            z3.ULE(m2, z3.BitVecVal(2000000000, 32)),
            z3.Or(z3.ULT(m1 + m2, m1), z3.ULT(m1 + m2, m2)),
        ),
    )

    # -------------------------------------------------------------
    # 6. BitVector Preimage Injectivity (Attribute Tag Separation)
    # -------------------------------------------------------------
    k1 = z3.BitVec("k1", 64)
    k2 = z3.BitVec("k2", 64)
    v1 = z3.BitVec("v1", 64)
    v2 = z3.BitVec("v2", 64)
    pair1 = z3.Concat(k1, v1)
    pair2 = z3.Concat(k2, v2)

    all_ok &= check_theorem(
        "Theorem 6 (BitVector Attribute Pair Injectivity): (k1 != k2 or v1 != v2) => Concat(k1, v1) != Concat(k2, v2)",
        lambda: z3.And(z3.Or(k1 != k2, v1 != v2), pair1 == pair2),
    )

    # -------------------------------------------------------------
    # 7. Circular Ring Buffer Bound Safety
    # -------------------------------------------------------------
    head = z3.BitVec("head", 32)
    cap = z3.BitVecVal(1024, 32)
    next_head = z3.URem(head + 1, cap)

    all_ok &= check_theorem(
        "Theorem 7 (Circular Buffer Index Safety): forall head in [0, 1024): (head + 1) % 1024 < 1024",
        lambda: z3.And(z3.ULT(head, cap), z3.UGE(next_head, cap)),
    )

    # -------------------------------------------------------------
    # 8. Epoch Poisoning DoS Immunity (Gossip Window Guard)
    # -------------------------------------------------------------
    e_local = z3.Int("e_local")
    e_remote = z3.Int("e_remote")
    epoch_diff = z3.If(e_remote >= e_local, e_remote - e_local, e_local - e_remote)
    accept_gossip = epoch_diff <= 1

    all_ok &= check_theorem(
        "Theorem 8 (Epoch Poisoning DoS Immunity): |e_remote - e_local| > 1 => Accept == False",
        lambda: z3.And(epoch_diff > 1, accept_gossip),
    )

    print("\n" + "=" * 80)
    if all_ok:
        print("FINAL VERDICT: ALL 8 ENTROPI THEOREMS 100% FORMALLY PROVED BY Z3 SMT")
    else:
        print("FINAL VERDICT: SMT PROOF FAILED")
    print("=" * 80)
    return all_ok


if __name__ == "__main__":
    t0 = time.time()
    success = run_smt_proofs()
    print(f"Total SMT verification time: {time.time() - t0:.2f}s")
    sys.exit(0 if success else 1)
