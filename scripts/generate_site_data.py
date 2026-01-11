import os
import json
import pandas as pd
import numpy as np


def to_payload(df_slice, bins=50):
    if df_slice.empty:
        return None

    earn = pd.to_numeric(df_slice["EARN_MDN_1YR"], errors="coerce").dropna()
    debt = pd.to_numeric(df_slice["DEBT_ALL_STGP_ANY_MDN"], errors="coerce").dropna()
    rpy = pd.to_numeric(df_slice["RPY_3YR_RT"], errors="coerce").dropna()

    # If we don't have the primary outcomes, skip this slice to avoid N/A in dashboard
    if earn.empty or debt.empty or rpy.empty:
        return None

    payload = {}

    counts, edges = np.histogram(earn.values, bins=bins)
    payload["earn_bins"] = [
        {
            "x0": round(float(edges[i]), 2),
            "x1": round(float(edges[i + 1]), 2),
            "count": int(counts[i]),
        }
        for i in range(len(counts))
    ]
    payload["earn_pcts"] = {
        f"p{k}": round(float(np.percentile(earn.values, k)), 2)
        for k in (10, 25, 50, 75, 90)
    }
    payload["debt_mdn"] = round(float(debt.median()), 2)
    payload["rpy_3yr_rt"] = round(float(rpy.mean()), 4)

    return payload


def generate_data(full_data=False):
    csv_path = "data/clean/clean_data.csv"
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found.")
        return

    df = pd.read_csv(csv_path, low_memory=False)

    if not full_data:
        selected_majors = [
            "Computer and Information Sciences, General.",
            "Registered Nursing, Nursing Administration, Nursing Research and Clinical Nursing.",
            "Business Administration, Management and Operations.",
            "Psychology, General.",
            "Biological and Biomedical Sciences, Other.",
        ]
        df = df[df["CIPDESC"].isin(selected_majors)]
    else:
        print("Processing all majors...")

    majors_all = sorted(df["CIPDESC"].dropna().unique())
    credlevs_all = sorted(df["CREDLEV"].dropna().astype(int).unique().tolist())

    data_dir = os.path.join("docs", "data")
    os.makedirs(data_dir, exist_ok=True)

    # Clean up old hashed files if any
    for f in os.listdir(data_dir):
        if f.endswith(".json") and f != "index.json":
            os.remove(os.path.join(data_dir, f))

    valid_majors = []
    valid_creds = set()
    all_payloads = {}

    for m in majors_all:
        m_slice = df[df["CIPDESC"] == m]
        m_payloads = {}

        p_all = to_payload(m_slice)
        if p_all:
            m_payloads["__ALL__"] = p_all

        for c in credlevs_all:
            c_slice = m_slice[m_slice["CREDLEV"] == c]
            p_c = to_payload(c_slice)
            if p_c:
                m_payloads[int(c)] = p_c
                valid_creds.add(int(c))

        if m_payloads:
            valid_majors.append(m)
            all_payloads[m] = m_payloads

    final_data = {
        "majors": sorted(valid_majors),
        "credlevs": sorted(list(valid_creds)),
        "payload": all_payloads,
    }

    output_path = os.path.join("docs", "data.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(final_data, f)

    # Remove hashed files directory if it exists
    if os.path.exists(data_dir):
        import shutil

        shutil.rmtree(data_dir)
        print(f"Removed {data_dir}")

    print(f"Wrote {len(valid_majors)} majors to {output_path}")


if __name__ == "__main__":
    import sys

    full = "--full" in sys.argv
    generate_data(full_data=full)
