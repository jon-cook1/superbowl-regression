"""
QCEW county–total data, 1975‑1989 (SIC).

Each yearly ZIP already holds the 4 quarterly CSVs.
The county‑level “total, all industries” record is identified by
  agglvl_code = 26  and  own_code = 0
"""
import io, zipfile, requests, pandas as pd
from pathlib import Path

YEARS  = range(1975, 1990)                        # 1975‑1989 inclusive
OUT    = Path("qcew_county_quarterly_75_89.csv")
FIELDS = ["area_fips", "area_title",
          "year", "qtr",
          "month1_emplvl", "month2_emplvl", "month3_emplvl",
          "total_qtrly_wages"]

rows = []

def sic_zip(year: int) -> str:
    return (f"https://data.bls.gov/cew/data/files/{year}/sic/csv/"
            f"sic_{year}_qtrly_by_area.zip")

for y in YEARS:
    url = sic_zip(y)
    print(f"→ {y}: {url}")
    try:
        z = zipfile.ZipFile(io.BytesIO(requests.get(url, timeout=60).content))
    except zipfile.BadZipFile:
        print(f"  ! {y} not available, skipping")
        continue

    for csv_name in z.namelist():                # four csvs per year
        if not csv_name.endswith(".csv"):
            continue
        with z.open(csv_name) as f:
            df = pd.read_csv(
                    f, dtype=str,
                    usecols=lambda c: c in FIELDS +
                                         ["own_code", "agglvl_code"]
                 )

        # keep county totals, all ownerships
        df = df[(df["own_code"] == "0") & (df["agglvl_code"] == "26")]
        rows.append(df[FIELDS])

    print(f"  kept {sum(len(r) for r in rows[-4:]):,} rows for {y}")

# save ------------------------------------------------------------------------
pd.concat(rows, ignore_index=True).to_csv(OUT, index=False)
print(f"\nSaved {OUT} with {OUT.stat().st_size/1_048_576:.1f} MB of data "
      f"and {sum(len(r) for r in rows):,} rows.")
