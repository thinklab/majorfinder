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

    # Helper to calculate specific gender slice
    def get_metrics(sub_df, earn_col, debt_col):
        res = {}
        earn_vals = pd.to_numeric(sub_df[earn_col], errors="coerce").dropna()
        debt_vals = pd.to_numeric(sub_df[debt_col], errors="coerce").dropna()

        if not earn_vals.empty:
            counts, edges = np.histogram(earn_vals.values, bins=bins)
            res["earn_bins"] = [
                {
                    "x0": round(float(edges[i]), 2),
                    "x1": round(float(edges[i + 1]), 2),
                    "count": int(counts[i]),
                }
                for i in range(len(counts))
            ]
            res["earn_pcts"] = {
                f"p{k}": round(float(np.percentile(earn_vals.values, k)), 2)
                for k in (10, 25, 50, 75, 90)
            }

        if not debt_vals.empty:
            res["debt_mdn"] = round(float(debt_vals.median()), 2)

        if not earn_vals.empty and not debt_vals.empty:
            common_idx = earn_vals.index.intersection(debt_vals.index)
            if not common_idx.empty:
                ratios = earn_vals.loc[common_idx] / debt_vals.loc[common_idx]
                ratios = ratios.replace([np.inf, -np.inf], np.nan).dropna()
                if not ratios.empty:
                    res["earn_to_debt_ratio"] = round(float(ratios.median()), 4)
        return res

    # Base outcomes
    payload = {
        "all": get_metrics(df_slice, "EARN_MDN_1YR", "DEBT_ALL_STGP_ANY_MDN"),
    }

    # Shared metrics (Institutional)
    rpy = pd.to_numeric(df_slice["RPY_3YR_RT"], errors="coerce").dropna()
    cost = pd.to_numeric(df_slice["COSTT4_A"], errors="coerce").dropna()

    if not rpy.empty:
        payload["rpy_3yr_rt"] = round(float(rpy.mean()), 4)
    if not cost.empty:
        payload["cost_avg"] = round(float(cost.mean()), 2)

    # Better Repayment Metrics (Longitudinal)
    def parse_rpy_val(val):
        """
        Parses BBRR values which can be:
        - Numbers (0.5)
        - Ranges ('0.1-0.2')
        - Comparisons ('<=0.05', '>=0.9')
        - Privacy Suppressed ('PS', 'PrivacySuppressed')
        """
        if pd.isna(val) or val == "" or val in ["PS", "PrivacySuppressed"]:
            return None

        s = str(val).strip()
        if not s:
            return None

        try:
            # Handle comparisons
            if s.startswith("<="):
                return float(s[2:])
            if s.startswith(">="):
                return float(s[2:])
            if s.startswith("<"):
                return float(s[1:])
            if s.startswith(">"):
                return float(s[1:])

            # Handle ranges
            if "-" in s:
                parts = s.split("-")
                v1 = float(parts[0].strip())
                v2 = float(parts[1].strip())
                return (v1 + v2) / 2.0

            # Handle simple numbers
            return float(s)
        except (ValueError, TypeError):
            return None

    def get_rpy_breakdown(year):
        n_col = f"BBRR{year}_FED_COMP_N"
        paid_col = f"BBRR{year}_FED_COMP_PAIDINFULL"
        prog_col = f"BBRR{year}_FED_COMP_MAKEPROG"
        dflt_col = f"BBRR{year}_FED_COMP_DFLT"
        dlnq_col = f"BBRR{year}_FED_COMP_DLNQ"
        fbr_col = f"BBRR{year}_FED_COMP_FBR"
        dfr_col = f"BBRR{year}_FED_COMP_DFR"

        cols = [n_col, paid_col, prog_col, dflt_col, dlnq_col, fbr_col, dfr_col]
        # Filter columns to only those present in df_slice
        available_cols = [c for c in cols if c in df_slice.columns]

        df_sub = df_slice[available_cols].copy()
        for col in available_cols:
            if col != n_col:
                df_sub[col] = df_sub[col].apply(parse_rpy_val)
            else:
                df_sub[col] = pd.to_numeric(df_sub[col], errors="coerce")

        # Drop rows where N or essential components are missing
        df_sub = df_sub.dropna(subset=[n_col])
        total_n = df_sub[n_col].sum()

        if total_n > 0:

            def weighted_rate(col):
                if col not in df_sub.columns:
                    return 0.0
                # Use only rows where both N and the rate col are available
                valid = df_sub[[n_col, col]].dropna()
                if valid.empty:
                    return 0.0
                return (valid[col] * valid[n_col]).sum() / valid[n_col].sum()

            paid_rate = weighted_rate(paid_col)
            prog_rate = weighted_rate(prog_col)
            dflt_rate = weighted_rate(dflt_col)
            dlnq_rate = weighted_rate(dlnq_col)
            fbr_rate = weighted_rate(fbr_col)
            dfr_rate = weighted_rate(dfr_col)

            # Map to groups
            healthy = paid_rate + prog_rate
            bad = dflt_rate + dlnq_rate
            other = fbr_rate + dfr_rate

            # Normalize if they exceed 1.0 (though they shouldn't if data is clean)
            total = healthy + bad + other
            if total > 1.0:
                healthy /= total
                bad /= total
                other /= total
            elif total < 1.0 and total > 0:
                # If we have some data but it doesn't add to 1, we could potentially
                # scale it or just leave it. Usually, there are other categories
                # not captured.
                pass

            return {
                "healthy": round(healthy, 4),
                "dist": {
                    "paid_prog": round(healthy, 4),
                    "default_delinq": round(bad, 4),
                    "other": round(max(0.0, 1.0 - healthy - bad), 4),
                },
            }
        return None

    rpy1 = get_rpy_breakdown(1)
    rpy4 = get_rpy_breakdown(4)

    if rpy1:
        payload["rpy1"] = rpy1
    if rpy4:
        payload["rpy4"] = rpy4

    # If all views are empty and no shared metrics, return None
    if not any(payload["all"].values()) and not rpy.empty and not cost.empty:
        pass  # keep it for cost/rpy
    elif not any(payload["all"].values()) and rpy.empty and cost.empty:
        return None

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
