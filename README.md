# search-engine

### IMDB data dumps
https://datasets.imdbws.com/

## Notes & suggestions
* The service builds a Solr q expression that searches title and description. If you prefer more advanced relevance (edismax), you can call the Solr /select with defType=edismax and pass qf and other parameters — I kept it simple and portable.
* For production, add:
    * input sanitization and stronger escaping,
    * caching for repeated queries,
    * authentication & rate-limiting,
    * connection pooling (requests.Session()).

* If you want autosuggestions, faceted drill-down UI helpers, or highlighting, I can add endpoints for:
    * /suggest?q=... using Suggester,
    * /facets that returns counts and top terms,
    * and server-side caching with TTL.