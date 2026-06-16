import sys, numpy as np, polars as pl
n=int(sys.argv[1]); k=int(sys.argv[2]); out=sys.argv[3]
rng=np.random.default_rng(7)
cols={"student_id": rng.integers(1,10_000_001,n), "school_id": rng.integers(1,10001,n)}
for i in range(1,k+1):
    cols[f"metric_{i:02d}"]=rng.integers(0,101,n)
pl.DataFrame(cols).write_csv(out)
print(f"wrote {n:,} rows x {k} value cols -> {out}")
