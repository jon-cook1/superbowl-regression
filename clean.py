import pandas as pd
import numpy as np

# filenames – adjust if needed
file1    = "qcew_county_quarterly_75_89.csv"
file2    = "qcew_county_quarterly_90_25.csv"
out_file = "2000_2024_county_clean.csv"

# toggle development filter
DROP_PRE_2000 = True     # set to True to drop all rows before year 2000

# toggle control subset filter
SUBSET_CONTROLS = False  # set to True to keep only host counties and selected controls

# list of FIPS codes to include as controls when SUBSET_CONTROLS=True
control_fips = [
    '24510', '36029', '47037', '55009', '53033', '42003', '39035', '42101', '39061', '17031', '37119', '25025', '24033', '29095', 
]

# 1. load and combine
df1 = pd.read_csv(file1, dtype=str)
df2 = pd.read_csv(file2, dtype=str)
df  = pd.concat([df1, df2], ignore_index=True)
total = len(df)

# 1.a initialize host_year as NaN
df['host_year'] = np.nan

# 1.b populate host_year from a dict of {area_fips: host_year}
host_dict = {
    '12086': 2020,  # Miami
    '22071': 2013,  # New Orleans
    '06037': 2022,  # LA
    '12057': 2021,  # Tampa
    '04013': 2015,  # Phoenix
    '06073': 2003,  # San Diego
    '48201': 2017,  # Houston
    '13121': 2019,  # Atlanta
    '26125': 2006,  # Detroit
    '06085': 2016,  # SF
    '27053': 2018,  # Minneapolis
    '12031': 2005,  # Jacksonville
    '48439': 2011,  # Dallas
    '18097': 2012,  # Indianapolis
    '34003': 2014,  # New York (East Rutherford)
}
df['host_year'] = df['area_fips'].map(host_dict)

# 2. keep only “county” or “parish”
mask_area    = df["area_title"].str.contains(r"county|parish", case=False, na=False)
df_area      = df[mask_area]
removed_area = total - len(df_area)

# 3. drop any rows with zero in the value columns
value_cols = ["month1_emplvl", "month2_emplvl", "month3_emplvl", "total_qtrly_wages"]
df_area[value_cols] = df_area[value_cols].apply(pd.to_numeric, errors="coerce")
mask_nonzero = (df_area[value_cols] != 0).all(axis=1)
df_clean     = df_area[mask_nonzero]
removed_zero = len(df_area) - len(df_clean)

# 3.a development filter: drop rows before year 2000 if DROP_PRE_2000=True
if DROP_PRE_2000:
    before_dev = len(df_clean)
    df_clean['year'] = df_clean['year'].astype(int)
    df_clean = df_clean[df_clean['year'] >= 2000]
    removed_dev = before_dev - len(df_clean)
else:
    removed_dev = 0

# 3.b subset controls: keep only host rows and selected control FIPS if SUBSET_CONTROLS=True
if SUBSET_CONTROLS:
    before_subset = len(df_clean)
    df_clean = df_clean[
        df_clean['host_year'].notna() |
        df_clean['area_fips'].isin(control_fips)
    ]
    removed_subset = before_subset - len(df_clean)
else:
    removed_subset = 0

# 4. report
remaining = len(df_clean)
print(f"Removed {removed_area} rows without 'county' or 'parish' in title")
print(f"Removed {removed_zero} rows with zero in any value column")
if DROP_PRE_2000:
    print(f"Dev filter removed {removed_dev} rows before year 2000")
if SUBSET_CONTROLS:
    print(f"Subset filter removed {removed_subset} rows not host or in control list")
print(f"{remaining} rows remain out of {total}")

print("\n----------\n")
host_counts = (
    df_clean[df_clean['host_year'].notna()]
    .groupby(['area_fips','area_title'])['host_year']
    .count()
    .reset_index(name='n_rows')
    .sort_values('n_rows', ascending=False)
)
print(host_counts)

# 5. save (original files untouched)
df_clean.to_csv(out_file, index=False)
