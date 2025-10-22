import requests
import pandas as pd
import os

# --- Config ---
API_KEY = "980b94ad26979097b1633a309a58c6d0"
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
OUTPUT_DIR = "data/fred_combined"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Correct state-level unemployment rate series IDs ---
unemployment_series = {
    "AL": "ALUR","AK": "AKUR","AZ": "AZUR","AR": "ARUR","CA": "CAUR","CO": "COUR",
    "CT": "CTUR","DE": "DEUR","FL": "FLUR","GA": "GAUR","HI": "HIUR","ID": "IDUR",
    "IL": "ILUR","IN": "INUR","IA": "IAUR","KS": "KSUR","KY": "KYUR","LA": "LAUR",
    "ME": "MEUR","MD": "MDUR","MA": "MAUR","MI": "MIUR","MN": "MNUR","MS": "MSUR",
    "MO": "MOUR","MT": "MTUR","NE": "NEUR","NV": "NVUR","NH": "NHUR","NJ": "NJUR",
    "NM": "NMUR","NY": "NYUR","NC": "NCUR","ND": "NDUR","OH": "OHUR","OK": "OKUR",
    "OR": "ORUR","PA": "PAUR","RI": "RIUR","SC": "SCUR","SD": "SDUR","TN": "TNUR",
    "TX": "TXUR","UT": "UTUR","VT": "VTUR","VA": "VAUR","WA": "WAUR","WV": "WVUR",
    "WI": "WIUR","WY": "WYUR"
}

# --- National monthly indicators (ICSA REMOVED) ---
national_series = [
    "CPIAUCSL", "CUSR0000SAF11", "CUSR0000SETB01", "PI", "PCE",
    "PSAVERT", "CES0500000003", "UMCSENT", "FEDFUNDS", "REVOLSL",
    "RSAFS", "ECOMNSA", "JTSJOL"
]

# --- Fetch one series ---
def fetch_series(series_id):
    try:
        r = requests.get(BASE_URL, params={
            "series_id": series_id,
            "api_key": API_KEY,
            "file_type": "json"
        })
        r.raise_for_status()
        data = r.json().get("observations", [])
        if not data:
            print(f"⚠️ No data for {series_id}")
            return None

        df = pd.DataFrame(data)[["date", "value"]]
        df["series_id"] = series_id
        df = df[(df["date"] >= "2018-01-01") & (df["date"] <= "2022-12-31")]
        return df

    except Exception as e:
        print(f"Error fetching {series_id}: {e}")
        return None

# --- Run extraction ---
all_dfs = []
for sid in list(unemployment_series.values()) + national_series:
    df = fetch_series(sid)
    if df is not None and not df.empty:
        all_dfs.append(df)

if not all_dfs:
    raise SystemExit("❌ No data downloaded.")

full_df = pd.concat(all_dfs, ignore_index=True)

# --- Convert numeric values ---
full_df["value"] = pd.to_numeric(full_df["value"], errors="coerce")

# --- Split macro vs unemployment ---
unemp = full_df[full_df["series_id"].str.endswith("UR")].copy()
macro = full_df[~full_df["series_id"].str.endswith("UR")].copy()

# --- Combine unemployment (long format) ---
unemp["state"] = unemp["series_id"].str[:2]
unemp = unemp.rename(columns={"value": "unemployment_rate"})[["date", "state", "unemployment_rate"]]
unemp.to_csv(os.path.join(OUTPUT_DIR, "fred_unemployment_state.csv"), index=False)

# --- Combine macro (wide format) ---
macro_wide = macro.pivot_table(index="date", columns="series_id", values="value").reset_index()
macro_wide.to_csv(os.path.join(OUTPUT_DIR, "fred_macro_combined.csv"), index=False)

print("\n✅ Combined files written:")
print(f" - {len(unemp)} rows → fred_unemployment_state.csv")
print(f" - {len(macro_wide)} rows × {len(macro_wide.columns)} cols → fred_macro_combined.csv")
