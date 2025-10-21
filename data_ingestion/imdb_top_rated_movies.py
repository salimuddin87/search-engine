#!/usr/bin/env python3
"""
Generate a CSV of the top N IMDb-rated films using IMDb's official datasets.

Prereqs:
    pip install pandas requests tqdm

Run:
    python imdb_top_rated_movies.py --top 10000 --min-votes 1000

Notes:
 - IMDb data is available at https://datasets.imdbws.com/
 - This script downloads the gz files directly and processes them locally.
"""

import argparse
import certifi
import os
import requests
from tqdm import tqdm
import pandas as pd

IMDB_BASE = "https://datasets.imdbws.com"
FILES = {
    "basics": "title.basics.tsv.gz",
    "ratings": "title.ratings.tsv.gz",
}


def download_file(fname, dest_folder="data"):
    os.makedirs(dest_folder, exist_ok=True)
    url = f"{IMDB_BASE}/{fname}"
    dest = os.path.join(dest_folder, fname)
    if os.path.exists(dest):
        print(f"Using cached file: {dest}")
        return dest
    print(f"Downloading {url} -> {dest}")
    # verify=certifi.where() should be used in production
    resp = requests.get(url, stream=True, timeout=60, verify=False)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))
    with open(dest, "wb") as f, tqdm(total=total, unit='B', unit_scale=True, desc=fname) as pbar:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                pbar.update(len(chunk))
    return dest


def load_basics(path):
    # title.basics.tsv.gz columns:
    # tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres
    print("Loading basics (may take a minute)...")
    df = pd.read_csv(
        path,
        sep="\t",
        compression="gzip",
        dtype={
            "tconst": str,
            "titleType": str,
            "primaryTitle": str,
            "originalTitle": str,
            "isAdult": str,
            "startYear": str,
            "endYear": str,
            "runtimeMinutes": str,
            "genres": str,
        },
        na_values=["\\N"],
        keep_default_na=False,
    )
    # Convert some columns
    df["startYear"] = pd.to_numeric(df["startYear"], errors="coerce").astype("Int64")
    df["runtimeMinutes"] = pd.to_numeric(df["runtimeMinutes"], errors="coerce").astype("Int64")
    return df


def load_ratings(path):
    # title.ratings.tsv.gz columns: tconst, averageRating, numVotes
    print("Loading ratings...")
    df = pd.read_csv(
        path,
        sep="\t",
        compression="gzip",
        dtype={"tconst": str, "averageRating": float, "numVotes": int},
        na_values=["\\N"],
    )
    return df


def build_top_movies(basics_df, ratings_df, top_n=10000, min_votes=None):
    # Restrict to movies only
    movies = basics_df[basics_df["titleType"] == "movie"].copy()
    print(f"Total titles with type 'movie': {len(movies):,}")
    # Merge with ratings
    merged = movies.merge(ratings_df, on="tconst", how="inner")
    print(f"Titles after merge with ratings: {len(merged):,}")
    # Optional: apply minimum votes threshold
    if min_votes:
        merged = merged[merged["numVotes"] >= min_votes].copy()
        print(f"After applying min_votes={min_votes}: {len(merged):,}")
    # Sort by averageRating, tie-break by numVotes
    merged = merged.sort_values(by=["averageRating", "numVotes"], ascending=[False, False])
    # Select top N
    top = merged.head(top_n).copy()
    # Clean / reorder columns for CSV output
    cols = [
        "tconst", "primaryTitle", "originalTitle", "startYear", "runtimeMinutes", "genres",
        "averageRating", "numVotes"
    ]
    for c in cols:
        if c not in top.columns:
            top[c] = pd.NA
    top = top[cols]
    # Some users prefer film title + year column
    top["title_with_year"] = top.apply(
        lambda r: f"{r['primaryTitle']} ({r['startYear']})" if pd.notna(r["startYear"]) else r["primaryTitle"],
        axis=1
    )
    # Reorder final columns
    out_cols = ["tconst", "title_with_year", "primaryTitle", "originalTitle", "startYear", "runtimeMinutes", "genres", "averageRating", "numVotes"]
    return top[out_cols]


def main():
    parser = argparse.ArgumentParser(description="Build top-N IMDb movies CSV using official IMDb datasets.")
    parser.add_argument("--top", type=int, default=10000, help="Number of top films to output.")
    parser.add_argument("--min-votes", type=int, default=0, help="Minimum number of votes required (0 = no filter).")
    parser.add_argument("--out", type=str, default="top_10000_imdb_movies.csv", help="Output CSV filename.")
    parser.add_argument("--cache-folder", type=str, default="data", help="Folder to save downloaded files.")
    args = parser.parse_args()

    basics_path = download_file(FILES["basics"], dest_folder=args.cache_folder)
    ratings_path = download_file(FILES["ratings"], dest_folder=args.cache_folder)

    basics_df = load_basics(basics_path)
    ratings_df = load_ratings(ratings_path)

    top_df = build_top_movies(basics_df, ratings_df, top_n=args.top, min_votes=(args.min_votes or None))
    top_df.to_csv(args.out, index=False)
    print(f"Wrote {len(top_df):,} rows to {args.out}")


if __name__ == "__main__":
    main()
