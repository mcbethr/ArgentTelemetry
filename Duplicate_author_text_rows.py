#!/usr/bin/env python3
"""
Find repeated (author, text) pairs and export ALL matching rows.

- Reads an Excel sheet (default: "Cross-Channel Telemetry")
- Finds rows where the SAME author posted the SAME text more than once
- Exports those rows to a new Excel file

Usage:
  python3 export_duplicate_author_text_rows.py \
    --infile "CrossChannelAuthors-2026-02-14_230239.xlsx" \
    --sheet "Cross-Channel Telemetry" \
    --outfile "DuplicateAuthorTextRows.xlsx"
"""

import argparse
import os
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True, help="Input .xlsx file")
    ap.add_argument("--sheet", default="Cross-Channel Telemetry", help="Sheet name to read")
    ap.add_argument("--author-col", default="author", help="Author column name")
    ap.add_argument("--text-col", default="text", help="Text column name")
    ap.add_argument("--outfile", default="DuplicateAuthorTextRows.xlsx", help="Output .xlsx file")
    ap.add_argument("--no-trim", action="store_true", help="Do NOT trim whitespace from text before comparing")
    args = ap.parse_args()

    df = pd.read_excel(args.infile, sheet_name=args.sheet)

    if args.author_col not in df.columns or args.text_col not in df.columns:
        raise SystemExit(
            f"Missing required columns. Found columns:\n{list(df.columns)}\n"
            f"Expected: '{args.author-col}' and '{args.text_col}'"
        )

    # Normalize author/text for matching
    work = df.copy()
    work[args.author_col] = work[args.author_col].astype(str).fillna("").str.strip()

    if args.no_trim:
        work["_text_norm"] = work[args.text_col].astype(str).fillna("")
    else:
        work["_text_norm"] = work[args.text_col].astype(str).fillna("").str.strip()

    # Find duplicates: same author AND same text_norm
    dup_mask = work.duplicated(subset=[args.author_col, "_text_norm"], keep=False)
    dup_rows = work.loc[dup_mask].copy()

    # Helpful summary table
    summary = (
        dup_rows.groupby([args.author_col, "_text_norm"])
        .size()
        .reset_index(name="repeat_count")
        .sort_values("repeat_count", ascending=False)
    )

    # Drop helper column before exporting the duplicated rows
    dup_rows = dup_rows.drop(columns=["_text_norm"], errors="ignore")

    # Ensure output ends with .xlsx
    outfile = args.outfile
    if not outfile.lower().endswith(".xlsx"):
        outfile = os.path.splitext(outfile)[0] + ".xlsx"

    with pd.ExcelWriter(outfile, engine="openpyxl") as writer:
        dup_rows.to_excel(writer, index=False, sheet_name="Duplicate Rows")
        summary.to_excel(writer, index=False, sheet_name="Summary")

    print(f"Found {len(summary)} repeated (author,text) pairs.")
    print(f"Exported {len(dup_rows)} duplicated rows to: {outfile}")


if __name__ == "__main__":
    main()