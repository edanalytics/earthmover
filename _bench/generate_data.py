import sys, datetime
import numpy as np
import polars as pl

n = int(sys.argv[1])
out = sys.argv[2]
rng = np.random.default_rng(42)

start = datetime.date(2019, 8, 2)
end = datetime.date(2020, 6, 17)
ndays = (end - start).days + 1
date_strs = np.array([(start + datetime.timedelta(days=int(i))).isoformat() for i in range(ndays)])

df = pl.DataFrame({
    "day":        date_strs[rng.integers(0, ndays, n)],
    "school_id":  rng.integers(1, 10001, n),
    "session":    rng.integers(1, 21, n),
    "student_id": rng.integers(1, 10_000_001, n),
    "attended":   np.where(rng.random(n) < 0.995, "TRUE", "FALSE"),
    "duration":   (rng.integers(1, 62, n) * 30),
})
df.write_csv(out, separator="\t")
print(f"wrote {n:,} rows -> {out}")
