# index_to_solr.py
import requests
from pathlib import Path

SOLR_URL = "http://localhost:8983/solr"
DATA_DIR = Path("./")


def post_csv(collection, csv_path, commit=True):
    url = f"{SOLR_URL}/{collection}/update?commit={'true' if commit else 'false'}"
    headers = {"Content-Type": "text/csv; charset=utf-8"}
    with open(csv_path, "rb") as f:
        resp = requests.post(url, headers=headers, data=f)
    print(collection, resp.status_code, resp.text[:500])


if __name__ == "__main__":
    post_csv("films", DATA_DIR/"top_10000_imdb_movies.csv")
    # post_csv("users", DATA_DIR/"users.csv")
    print("Indexing done. Run a query to verify, e.g.:")
    print(f"{SOLR_URL}/films/select?q=*:*&rows=5")
