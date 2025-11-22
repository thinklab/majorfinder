import pandas as pd

# -------------------------------------------------------------------
# CONFIG — set your file paths here
# -------------------------------------------------------------------
INSTITUTION_FILE = "cohorts_institutions.csv"
FOS_FILE = "recent-cohorts-filed.csv"

# Minimal institution-level columns
institution_cols = [
    "UNITID",
    "INSTNM",
    "CONTROL",
    "DISTANCEONLY",
    "NPT4_PUB",
    "NPT4_PRIV",
    "COSTT4_A",
    "MD_EARN_WNE_P6",
    "MD_EARN_WNE_P10",
    "MD_EARN_WNE_5YR",
    "DEBT_MDN",
    "RPY_1YR_RT",
    "RPY_3YR_RT",
    "RPY_5YR_RT",
]

# Minimal field-of-study columns
fostudy_cols = [
    "UNITID",
    "CIPCODE",
    "CIPDESC",
    "CREDLEV",
    # Earnings
    "EARN_MDN_1YR",
    "EARN_MDN_4YR",
    "EARN_MDN_5YR",
    # Optional high-cred earnings
    "EARN_MDN_HI_1YR",
    "EARN_MDN_HI_2YR",
    # Pell / NoPell earnings
    "EARN_PELL_WNE_MDN_1YR",
    "EARN_NOPELL_WNE_MDN_1YR",
    # Male / NotMale earnings
    "EARN_MALE_WNE_MDN_1YR",
    "EARN_NOMALE_WNE_MDN_1YR",
    # Debt fields
    "DEBT_ALL_STGP_ANY_MDN",
    "DEBT_ALL_PP_ANY_MDN",
    "DEBT_ALL_PP_ANY_MDN10YRPAY",
    # Repayment components
    "BBRR1_FED_COMP_N",
    "BBRR1_FED_COMP_PAIDINFULL",
]


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def normalize_cip(cip: str) -> str:
    """
    Normalize a CIP code:
    - remove dots
    - pad to 6 digits
    """
    if pd.isna(cip):
        return pd.NA
    c = str(cip).replace(".", "")
    return c.zfill(6)


# -------------------------------------------------------------------
# Load institution-level data
# -------------------------------------------------------------------
def load_institution_data(path: str) -> pd.DataFrame:
    print("Loading institution-level data...")
    df = pd.read_csv(path, usecols=institution_cols, low_memory=False)
    return df


# -------------------------------------------------------------------
# Load field-of-study data (major-level)
# -------------------------------------------------------------------
def load_fos_data(path: str) -> pd.DataFrame:
    print("Loading field-of-study (FoS) data...")
    df = pd.read_csv(path, usecols=fostudy_cols, low_memory=False)

    # Normalize CIP codes
    print("Normalizing CIP codes...")
    df["CIPCODE_ORIG"] = df["CIPCODE"]
    df["CIPCODE"] = df["CIPCODE"].apply(normalize_cip)
    return df


# -------------------------------------------------------------------
# Merge datasets
# -------------------------------------------------------------------
def merge_datasets(fos: pd.DataFrame, inst: pd.DataFrame) -> pd.DataFrame:
    print("Merging FoS + Institution data...")
    df = fos.merge(inst, on="UNITID", how="left")
    print(f"Merged dataset shape: {df.shape}")
    return df


# -------------------------------------------------------------------
# Master function
# -------------------------------------------------------------------
def load_and_clean():
    fos = load_fos_data(FOS_FILE)
    inst = load_institution_data(INSTITUTION_FILE)
    merged = merge_datasets(fos, inst)
    print("\nDone. Clean dataset ready.")
    return merged


# -------------------------------------------------------------------
# Run as script
# -------------------------------------------------------------------
if __name__ == "__main__":
    df = load_and_clean()
    print(df.head())
    df.to_csv("clean_data.csv", index=False)
