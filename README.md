# rsimcosta

R client for the [SIMCOSTA](https://simcosta.furg.br) buoy data platform.

Downloads oceanographic and meteorological buoy data and caches it locally
in SQLite so repeated queries only fetch missing time windows.

## Installation

```r
# install.packages("pak")
pak::pak("leoniedu/rsimcosta")
```

## Usage

```r
library(rsimcosta)

res <- simcosta_fetch(
  boia_id = 515,
  start   = "2025-01-01",
  end     = "2025-01-02"
)

head(res$standard)
head(res$currents)
```

### Fetch a single endpoint

```r
waves <- simcosta_fetch(515, "2025-01-01", "2025-01-02", endpoint = "standard")
```

### Cache management

```r
# Check cache status
simcosta_cache_info()

# Clear cache for a specific buoy
simcosta_clear_cache(boia_id = 515)

# Clear everything
simcosta_clear_cache(everything = TRUE)
```

### Configuration

| Option | Env var | Default |
|---|---|---|
| `rsimcosta.api_url` | `SIMCOSTA_API_URL` | `https://simcosta.furg.br/api` |
| `rsimcosta.rate_limit` | -- | `1` (requests per second) |

## License

MIT
