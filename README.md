# Major Finder

This project analyzes U.S. Department of Education (College Scorecard, IPEDS) data to estimate employment probability, debt burden, and ROI by college major.

## Data Sources

The project utilizes data from the [College Scorecard](https://collegescorecard.ed.gov/data):
- **Institution-Level Data**: (`cohorts_institutions.csv`) Provides metrics such as repayment rates and overall institution stats.
- **Field of Study Data**: (`recent-cohorts-filed.csv`) Provides major-specific outcomes including median earnings and debt.

## Data Processing Pipeline

1.  **Extraction and Cleaning**: `scripts/load_and_clean.py`
    - Loads raw CSV files from `data/raw/`.
    - Normalizes CIP (Classification of Instructional Programs) codes to ensure consistency across datasets.
    - Merges institution-level data with field-of-study data using `UNITID`.
    - Outputs `data/clean/clean_data.csv`.

2.  **Aggregation for Dashboard**: `scripts/generate_site_data.py`
    - Filters the cleaned data for specific high-interest majors.
    - Aggregates earnings distribution (histograms) and calculates percentiles (p10, p25, p50, p75, p90).
    - Calculates average repayment rates and median debt.
    - Exports the processed results to `docs/data.json` for consumption by the web dashboard.

3.  **Visualization**: `docs/index.html`
    - A static HTML dashboard using Plotly.js to visualize the processed data.

## Live Demo

The dashboard is automatically deployed to GitHub Pages:
[View Live Dashboard](https://thinklab.github.io/majorfinder/)

## How to Run & Deploy

To keep the repository clean and minimize CI usage, data processing is performed locally.

1.  **Local Setup**:
    - Place raw data files in `data/raw/`.
    - Install dependencies: `pip install pandas numpy`.

2.  **Process Data**:
    - Run the cleaning script:
      ```bash
      python scripts/load_and_clean.py
      ```
    - Generate the dashboard data (this creates `docs/data.json`):
      - Quick (5 majors): `python scripts/generate_site_data.py`
      - Full (all majors): `python scripts/generate_site_data.py --full`

3.  **Deployment**:
    - The `docs/data/` partition strategy has been reverted to a single `docs/data.json` for simplicity.
    - The legacy partitioned data in `docs/data/` is no longer used.
    - Commit and push your changes, including the updated `docs/data.json`.
    - The GitHub Action will automatically deploy the contents of the `docs/` folder to GitHub Pages.
    - **Note**: The `data/` directory is excluded from the repository via `.gitignore` to keep it clean. Only the final `docs/data.json` is required for the dashboard.