"""
FastAPI Solr Search Service using edismax with fuzzy/title & phrase boosting.

Run:
  uvicorn search_service:app --host 0.0.0.0 --port 5000

Notes:
- Uses Solr edismax parser (defType=edismax).
- Title gets strong weight; directors/actors moderate; description lower weight.
- Phrase boosting via pf/pf2/pf3 to boost exact phrase matches.
- Optional fuzzy matching: when fuzzy=True, a fuzzy sub-clause on title is added.
- Basic function boosts:
    - bf: log(sum(vote_count,1)) to favour more voted items
    - bq: boost query to favour high average_rating (e.g. average_rating:[8 TO *]^5)
- Tune the qf/pf/bf/bq parameters to your needs.
"""
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import requests
import math
import os
import re

# === Config ===
SOLR_URL = os.getenv("SOLR_URL", "http://localhost:8983/solr")
SOLR_COLLECTION = os.getenv("SOLR_COLLECTION", "films")
SOLR_SELECT = f"{SOLR_URL.rstrip('/')}/{SOLR_COLLECTION}/select"

# Field names (adjust to your schema)
FIELD_ID = "id"
FIELD_TITLE = "title"
FIELD_YEAR = "year"
FIELD_GENRES = "genres"
FIELD_AVG_RATING = "average_rating"
FIELD_VOTE_COUNT = "vote_count"
FIELD_DIRECTORS = "directors"
FIELD_ACTORS = "actors"
FIELD_DESC = "description"


# === Helpers ===
def solr_escape_phrase(s: str) -> str:
    """
    Escape special characters for phrase or term use.
    For edismax, quoting the phrase is generally safe, but we still escape quotes/backslashes.
    """
    if s is None:
        return ""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def tokenize_for_fuzzy(s: str) -> List[str]:
    # very small tokenizer: split on whitespace and remove punctuation
    if not s:
        return []
    tokens = re.findall(r"\w+", s)
    return tokens


def build_q_param(q: Optional[str]) -> str:
    if not q:
        return "*:*"
    # We'll keep the bare q but rely on qf/pf. Use the raw phrase (quoted) and also unquoted version.
    q_esc = solr_escape_phrase(q)
    # edismax will apply qf to the unquoted portion; include phrase in quotes to favor phrase match.
    # Example: '"dark knight"'
    return f'"{q_esc}" OR {q_esc}'


def build_fq_filters(
    genres: Optional[List[str]],
    min_rating: Optional[float],
    max_rating: Optional[float],
    directors: Optional[List[str]],
    actors: Optional[List[str]],
    year_from: Optional[int],
    year_to: Optional[int],
) -> List[str]:
    fqs = []
    if genres:
        escaped = " OR ".join([f'"{solr_escape_phrase(g)}"' for g in genres])
        fqs.append(f"{FIELD_GENRES}:({escaped})")
    if directors:
        escaped = " OR ".join([f'"{solr_escape_phrase(d)}"' for d in directors])
        fqs.append(f"{FIELD_DIRECTORS}:({escaped})")
    if actors:
        escaped = " OR ".join([f'"{solr_escape_phrase(a)}"' for a in actors])
        fqs.append(f"{FIELD_ACTORS}:({escaped})")
    if min_rating is not None or max_rating is not None:
        low = "*" if min_rating is None else min_rating
        high = "*" if max_rating is None else max_rating
        fqs.append(f"{FIELD_AVG_RATING}:[{low} TO {high}]")
    if year_from is not None or year_to is not None:
        ylow = "*" if year_from is None else year_from
        yhigh = "*" if year_to is None else year_to
        fqs.append(f"{FIELD_YEAR}:[{ylow} TO {yhigh}]")
    return fqs


# === Response models ===
class Film(BaseModel):
    id: str
    title: Optional[str] = None
    year: Optional[int] = None
    genres: Optional[List[str]] = None
    average_rating: Optional[float] = None
    vote_count: Optional[int] = None
    directors: Optional[List[str]] = None
    actors: Optional[List[str]] = None
    description: Optional[str] = None
    _score: Optional[float] = None


class SearchResult(BaseModel):
    total: int
    page: int
    per_page: int
    total_pages: int
    results: List[Film]
    facets: Optional[Dict[str, Any]] = None


# === App ===
app = FastAPI(title="Solr edismax Search API", version="1.0")


@app.get("/search", response_model=SearchResult)
def search(
    q: Optional[str] = Query(None, description="Text query (searches title and description)"),
    genre: Optional[List[str]] = Query(None, description="Filter by genre (multi)"),
    director: Optional[List[str]] = Query(None, description="Filter by director (multi)"),
    actor: Optional[List[str]] = Query(None, description="Filter by actor (multi)"),
    min_rating: Optional[float] = Query(None, ge=0.0, le=10.0),
    max_rating: Optional[float] = Query(None, ge=0.0, le=10.0),
    year_from: Optional[int] = Query(None),
    year_to: Optional[int] = Query(None),
    sort: Optional[str] = Query(None, description="Sort expression, e.g. average_rating desc"),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=200),
    facet: bool = Query(False),
    fuzzy: bool = Query(False, description="Enable fuzzy matching on title (adds fuzzy sub-clause)"),
    fuzzy_distance: int = Query(2, ge=1, le=3, description="Fuzzy edit distance for fuzzy matching (~N)"),
):
    """
    Search endpoint using edismax with phrase boosting and optional fuzzy matching.
    """
    start = (page - 1) * per_page
    q_param = build_q_param(q)
    fqs = build_fq_filters(genre, min_rating, max_rating, director, actor, year_from, year_to)

    # Base edismax params
    params = {
        "defType": "edismax",
        "q": q_param,
        "start": start,
        "rows": per_page,
        "wt": "json",
        # Query fields + weights: title strongest, then directors, actors, description, genres
        # qf is applied to the unquoted portion of q; edismax will analyze the text per field.
        "qf": f"{FIELD_TITLE}^6 {FIELD_DIRECTORS}^3 {FIELD_ACTORS}^2 {FIELD_DESC}^1 {FIELD_GENRES}^1",
        # Phrase boosting: exact phrase matches in title highly rewarded
        "pf": f'{FIELD_TITLE}^8',     # exact phrase in title
        "pf2": f'{FIELD_TITLE}^4',    # 2-term phrase
        "pf3": f'{FIELD_TITLE}^2',    # 3-term phrase
        # Minimum should match: allow flexibility; this is a reasonable default
        "mm": "2<-1 3<75%",
        # tie breaker for dismax combining
        "tie": 0.1,
        # phrase slop (qs) helps short phrase matches tolerate slop
        "qs": 2,
        # highlight score to return 'score' in docs (Solr may put score in 'score' field if requested)
        "fl": ",".join([FIELD_ID, FIELD_TITLE, FIELD_YEAR, FIELD_GENRES, FIELD_AVG_RATING, FIELD_VOTE_COUNT, FIELD_DIRECTORS, FIELD_ACTORS, FIELD_DESC, "score"])
    }

    # Function boost (bf) and boost query (bq)
    # bf: prefer items with more votes (log to reduce effect); ensure field exists in schema
    # bq: prefer items with high average_rating
    # These are soft boosts: they influence ranking, not filtering.
    params["bf"] = f"log(sum({FIELD_VOTE_COUNT},1))"
    params["bq"] = f"{FIELD_AVG_RATING}:[8 TO *]^5"

    # Optional fuzzy sub-clause: we add a boost query (bq) that fuzzily matches title tokens.
    if fuzzy and q:
        tokens = tokenize_for_fuzzy(q)
        if tokens:
            # build fuzzy token list like: token1~2 token2~2 ...
            fuzzy_terms = " ".join([f"{re.sub(r'\"','',re.escape(t))}~{int(fuzzy_distance)}" for t in tokens])
            # Put it inside title:( ... )
            fuzzy_clause = f'{FIELD_TITLE}:({fuzzy_terms})'
            # Add as a boost query with lower weight to prefer fuzzy hits but not override exact matches
            # use ^1.5 or similar
            params.setdefault("bq", "")
            # If bq already exists, combine with || so Solr sees multiple bq entries when requests encodes lists.
            # We'll convert to a list later; for now, collect in a list structure.
            # To ensure requests encodes multiple bq params, we'll manage below.
            # Use a unique list container in local variable
            fuzzy_bq = f"{fuzzy_clause}^1.5"
        else:
            fuzzy_bq = None
    else:
        fuzzy_bq = None

    # Add filter queries: requests accepts list values for a key to emit multiple fq parameters
    if fqs:
        params["fq"] = fqs

    # Add facets if requested
    if facet:
        params["facet"] = "true"
        params["facet.field"] = [FIELD_GENRES, FIELD_DIRECTORS, FIELD_ACTORS]
        params["facet.limit"] = 20
        params["facet.mincount"] = 1

    # Sorting: pass-through
    if sort:
        params["sort"] = sort

    # Build final param lists for bq: initial bq from rating boost + fuzzy if present.
    # We want to support multiple bq params; build list if needed.
    bq_list = []
    # bq from rating (already set above)
    if "bq" in params and isinstance(params["bq"], str) and params["bq"]:
        bq_list.append(params["bq"])
    # add fuzzy bq
    if fuzzy_bq:
        bq_list.append(fuzzy_bq)
    # replace param with list if multiple
    if bq_list:
        params["bq"] = bq_list
    else:
        params.pop("bq", None)

    # At this stage params may contain list values for 'fq', 'facet.field', and 'bq'.
    # Use requests to make the GET call; requests will create multiple same-name params.
    try:
        r = requests.get(SOLR_SELECT, params=params, timeout=30)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Error connecting to Solr: {e}")

    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=f"Solr error: {r.text}")

    data = r.json()
    resp = data.get("response", {})
    num_found = resp.get("numFound", 0)
    docs = resp.get("docs", [])

    films = []
    for d in docs:
        film = Film(
            id=d.get(FIELD_ID),
            title=d.get(FIELD_TITLE),
            year=d.get(FIELD_YEAR),
            genres=d.get(FIELD_GENRES),
            average_rating=d.get(FIELD_AVG_RATING),
            vote_count=d.get(FIELD_VOTE_COUNT),
            directors=d.get(FIELD_DIRECTORS),
            actors=d.get(FIELD_ACTORS),
            description=d.get(FIELD_DESC),
            _score=d.get("score")
        )
        films.append(film)

    facets_out = None
    if facet:
        facets = data.get("facet_counts", {}).get("facet_fields", {})

        def facet_list_to_pairs(arr):
            if not arr:
                return []
            it = iter(arr)
            return [{"value": v, "count": next(it)} for v in it]
        facets_out = {k: facet_list_to_pairs(v) for k, v in facets.items()}

    total_pages = math.ceil(num_found / per_page) if per_page else 0
    return SearchResult(
        total=num_found,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        results=films,
        facets=facets_out
    )


@app.get("/film/{film_id}", response_model=Film)
def get_film(film_id: str):
    q = f'{FIELD_ID}:"{solr_escape_phrase(film_id)}"'
    params = {"q": q, "rows": 1, "wt": "json", "fl": ",".join([FIELD_ID, FIELD_TITLE, FIELD_YEAR, FIELD_GENRES, FIELD_AVG_RATING, FIELD_VOTE_COUNT, FIELD_DIRECTORS, FIELD_ACTORS, FIELD_DESC, "score"])}
    r = requests.get(SOLR_SELECT, params=params, timeout=10)
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=f"Solr error: {r.text}")
    data = r.json().get("response", {})
    docs = data.get("docs", [])
    if not docs:
        raise HTTPException(status_code=404, detail=f"Film {film_id} not found")
    d = docs[0]
    return Film(
        id=d.get(FIELD_ID),
        title=d.get(FIELD_TITLE),
        year=d.get(FIELD_YEAR),
        genres=d.get(FIELD_GENRES),
        average_rating=d.get(FIELD_AVG_RATING),
        vote_count=d.get(FIELD_VOTE_COUNT),
        directors=d.get(FIELD_DIRECTORS),
        actors=d.get(FIELD_ACTORS),
        description=d.get(FIELD_DESC),
        _score=d.get("score")
    )
