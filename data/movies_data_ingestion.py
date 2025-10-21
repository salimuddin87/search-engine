# index_to_solr.py
import requests
from pathlib import Path

SOLR_URL = "http://localhost:8983/solr"
DATA_DIR = Path("data_csv/")


def post_csv(collection, csv_path, commit=True):
    url = f"{SOLR_URL}/{collection}/update?commit={'true' if commit else 'false'}"
    headers = {"Content-Type": "text/csv; charset=utf-8"}
    with open(csv_path, "rb") as f:
        resp = requests.post(url, headers=headers, data=f)
    print(collection, resp.status_code, resp.text[:500])


if __name__ == "__main__":
    post_csv("movies", DATA_DIR/"movies.csv")
    post_csv("ratings", DATA_DIR/"ratings.csv")
    print("Indexing done. Run a query to verify, e.g.:")
    print(f"{SOLR_URL}/movies/select?q=*:*&rows=5")
