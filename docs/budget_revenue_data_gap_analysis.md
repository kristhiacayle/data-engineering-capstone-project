# Budget & Revenue Data Gap Analysis
**Pipeline Layer:** Silver (Enrichment)

---

## Executive Summary

The Silver enrichment stage attempted to fill missing `budget` and `revenue` fields across 45,433 movies. All viable external sources were evaluated — TMDB API, OMDb API, Cinemagoer/IMDb scraping, and The Numbers. TMDB was the only viable option and was executed. The remaining gaps exist because the data is not publicly available — not because of a tooling limitation. Financial analysis in the Gold layer is scoped to the subset of movies where verified data exists.

---

## 1. The Problem

The source dataset contains **45,486 movies** (1874–2020). Financial fields in the Bronze layer were severely incomplete. Values of `0` are treated as unknown — a production budget of zero dollars is not meaningful.

**Budget:**
- Bronze with real values (not NULL/0): 7,164
- Bronze missing (NULL or 0): 38,322 (84.2%)

**Revenue:**
- Bronze with real values (not NULL/0): ~0 (virtually all stored as "0.0")
- Bronze missing: ~45,480 (effectively 100%)

Budget and revenue are the core fields for downstream ROI analysis, profitability segmentation, and genre-level financial performance. The question: can external sources fill these gaps?

---

## 2. Enrichment — TMDB API (Executed)

### 2.1 Method

TMDB provides a `/movie/{id}` endpoint with `budget` and `revenue` as integers (USD). The dataset uses TMDB IDs as the primary key, so every call was a direct ID lookup — no title matching required. Two API keys were used in parallel with 40 concurrent threads to maximize throughput. **38,332 candidate movies** were queried.

### 2.2 Results

**Budget:**
- Bronze with real values: 7,164
- Enrichment added: +5,216
- Silver total with real values: **12,380**
- Coverage: 15.8% → **27.2%** of all movies now have budget data

**Revenue:**
- Bronze with real values: ~0
- Enrichment added: +11,548
- Silver total with real values: **11,548**
- Coverage: ~0% → **25.4%** of all movies now have revenue data

**Genres:**
- Bronze with real values: 43,024
- Enrichment added: +2,125
- Silver total with real values: **45,149**
- Coverage: 94.7% → **99.4%** of all movies now have genre data

### 2.3 Why the fill rate is low for financials

TMDB's financial data is community-contributed. Large studio films are well-maintained. Independent films, foreign releases, documentaries, streaming originals, and direct-to-video titles are almost never filled — contributors have no access to non-public financial figures. This is why genres (publicly verifiable) reached 98.2% fill while budget/revenue stayed low.

---

## 3. Other Sources Evaluated

### OMDb API — Not Used

OMDb has no `budget` field at all. Its only financial field is `BoxOffice` — US domestic gross only, returned as a formatted string (e.g. `"$292,587,330"`), with `"N/A"` for most films. This is a different metric from the worldwide revenue already in TMDB. Using it would introduce inconsistency, not improvement.

Additionally: free tier is 1,000 requests/day (33+ days to cover remaining nulls), maintained by a single developer with no SLA.

**Decision: Not used.**

### Cinemagoer / IMDb Scraping — Not Used

Cinemagoer (formerly IMDbPY) scrapes IMDb HTML. IMDb does have budget data, but:

1. **Currently broken.** GitHub Issue #537 (June 2025, still open March 2026): IMDb redesigned its `/reference` page and broke all parsers. Tested empirically — The Matrix, The Dark Knight, and The Shawshank Redemption all returned empty dicts.
2. **Historically fragile.** Has broken on every major IMDb redesign. Fixes depend on volunteers, taking weeks to months.

**Decision: Not used.**

### The Numbers — Not Used

The-numbers.com has the most reliable public budget data (sourced from Comscore and studio filings), but:

1. **Narrow coverage.** ~5,200 movies in their entire database vs. 45,433 in ours. Estimated yield: 1,600–2,500 additional rows — not enough to be meaningful.
2. **Site mid-rebuild** as of March 2026. Budget list endpoints return Cloudflare challenge pages or 404s.
3. **No ID matching.** Matching must be done by title + year — unreliable with foreign titles, remakes, and special characters.

**Decision: Not used.**

---

## 4. Why the Data Doesn't Exist

Production budgets are proprietary. There is no legal requirement to disclose them. The data that does exist publicly comes from voluntary studio disclosures, journalist estimates, or court filings — almost exclusively for major theatrical releases.

A 2024 study by film researcher Stephen Follows (62,298 films since 2000) found budget data is publicly available for only **10.7% of all films** — dropping to **3.3% for films from the past five years**. Independent productions, streaming originals, foreign films, and documentaries almost never have public financial data. No engineering solution can fill data that was never disclosed.

---

## 5. Final Decision

No further enrichment will be attempted. The Silver layer represents the maximum recoverable completeness from all publicly available sources.

**Final Silver financial completeness:**
- Movies with confirmed budget: 12,380 (27.2%)
- Movies with confirmed revenue: 11,548 (25.4%)
- Movies with both confirmed: **7,999 (17.6%)**

The Gold layer will implement explicit data tiering (`budget_tier = 'known'` / `'unknown'`). All financial analyses (ROI, profitability by genre, budget efficiency) will be scoped to the **7,999 movies with both fields confirmed**. Analyzing the population where data exists — rather than imputing or approximating — is standard practice in financial analytics and produces honest, defensible results.

---

## 6. Sources Evaluated — Summary

| Source | Has Budget | Has Revenue | Est. Coverage | Status | Decision |
|---|---|---|---|---|---|
| **TMDB API** | Yes | Yes | 27% after enrichment | Executed | Used |
| **OMDb API** | No | US only | <10% revenue | Evaluated | Not used |
| **Cinemagoer (IMDb)** | Yes | Yes | ~30–40% (when working) | Broken (Issue #537) | Not used |
| **The Numbers** | Yes | Yes | 8–15% of nulls | Site mid-rebuild | Not used |
| **IMDb official API** | No public API | No public API | — | Paid/gated | Not applicable |

