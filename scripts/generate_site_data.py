import os
import json
import pandas as pd
import numpy as np


CIP_GROUPS = {
    "01": "Agriculture",
    "03": "Natural Resources",
    "04": "Architecture",
    "05": "Area/Ethnic/Gender Studies",
    "09": "Communication/Journalism",
    "10": "Communications Technologies",
    "11": "Computer Sciences",
    "12": "Personal/Culinary Services",
    "13": "Education",
    "14": "Engineering",
    "15": "Engineering Technologies",
    "16": "Foreign Languages",
    "19": "Family/Consumer Sciences",
    "22": "Legal Professions",
    "23": "English Language/Literature",
    "24": "Liberal Arts/Humanities",
    "25": "Library Science",
    "26": "Biological/Biomedical Sciences",
    "27": "Mathematics/Statistics",
    "28": "Military Science",
    "29": "Military Technologies",
    "30": "Multi/Interdisciplinary Studies",
    "31": "Parks/Recreation/Leisure/Fitness",
    "38": "Philosophy/Religion",
    "39": "Theology/Religious Vocations",
    "40": "Physical Sciences",
    "41": "Science Technologies",
    "42": "Psychology",
    "43": "Homeland Security/Law Enforcement",
    "44": "Public Administration/Social Service",
    "45": "Social Sciences",
    "46": "Construction Trades",
    "47": "Mechanic/Repair Technologies",
    "48": "Precision Production",
    "49": "Transportation/Materials Moving",
    "50": "Visual and Performing Arts",
    "51": "Health Professions",
    "52": "Business/Marketing",
    "54": "History",
    "60": "Residency Programs",
}


def to_payload(df_slice, bins=50):
    if df_slice.empty:
        return None

    earn = pd.to_numeric(df_slice["EARN_MDN_1YR"], errors="coerce").dropna()
    debt = pd.to_numeric(df_slice["DEBT_ALL_STGP_ANY_MDN"], errors="coerce").dropna()
    rpy = pd.to_numeric(df_slice["RPY_3YR_RT"], errors="coerce").dropna()
    cost = pd.to_numeric(df_slice["COSTT4_A"], errors="coerce").dropna()

    # If we don't have the primary outcomes, skip this slice to avoid N/A in dashboard
    if earn.empty and debt.empty and rpy.empty:
        return None

    payload = {}

    if not earn.empty:
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

    if not debt.empty:
        payload["debt_mdn"] = round(float(debt.median()), 2)

    if not rpy.empty:
        payload["rpy_3yr_rt"] = round(float(rpy.mean()), 4)

    if not cost.empty:
        payload["cost_avg"] = round(float(cost.mean()), 2)

    if not earn.empty and not debt.empty:
        # Calculate individual ratios then take median to avoid skew by outlier schools
        # or just take ratio of medians? Ratio of medians is more common for this high-level view.
        # But individual ratios median is better.
        common_idx = earn.index.intersection(debt.index)
        if not common_idx.empty:
            ratios = earn.loc[common_idx] / debt.loc[common_idx]
            # Handle cases where debt might be 0 to avoid inf
            ratios = ratios.replace([np.inf, -np.inf], np.nan).dropna()
            if not ratios.empty:
                payload["earn_to_debt_ratio"] = round(float(ratios.median()), 4)

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
    major_to_group = {}
    valid_creds = set()
    all_payloads = {}

    for m in majors_all:
        m_slice = df[df["CIPDESC"] == m]
        m_payloads = {}

        # Determine group
        cip_code = m_slice["CIPCODE"].iloc[0]
        cip2 = f"{int(cip_code):04d}"[:2]
        group_name = CIP_GROUPS.get(cip2, "Other")

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
            major_to_group[m] = group_name

    # Build grouped structure and group payloads
    groups = {}
    group_payloads = {}
    for g, ms in sorted(major_to_group.items()):  # Wait, major_to_group is major: group
        pass

    # Correct way to iterate groups
    unique_groups = sorted(set(major_to_group.values()))
    for g in unique_groups:
        ms_in_group = [m for m, grp in major_to_group.items() if grp == g]
        groups[g] = sorted(ms_in_group)

        # Calculate group-level payload
        g_slice = df[df["CIPDESC"].isin(ms_in_group)]
        g_payloads = {}
        p_all = to_payload(g_slice)
        if p_all:
            g_payloads["__ALL__"] = p_all

        for c in credlevs_all:
            c_slice = g_slice[g_slice["CREDLEV"] == c]
            p_c = to_payload(c_slice)
            if p_c:
                g_payloads[int(c)] = p_c

        if g_payloads:
            group_payloads[g] = g_payloads

    final_data = {
        "groups": groups,
        "group_payloads": group_payloads,
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
