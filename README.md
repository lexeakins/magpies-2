# Magpie v2

Magpie is a local experiment app for finding and evaluating company website matches from a source list.

The source identity is the company name, city, state, and country from the uploaded file. Magpie tries to find the best website candidate for that identity and explain why the match is strong, weak, or unresolved.

## What The App Does

1. Upload an Excel or CSV source file.
2. Detect and clean the company/location columns.
3. Drop rows that do not have the minimum source identity fields.
4. Search Google Maps for each company near the expected location.
5. Optionally check manually verified known URL associations.
6. Optionally check Salesforce and the legacy database for prior website evidence.
7. Optionally ask Perplexity to find or verify a website candidate.
8. Optionally ask Haiku to inspect page text.
9. Score each candidate URL with deterministic identity rules.
10. Export the enriched results and run summary.

## Business Explanation Of Confidence

The confidence score answers one question:

> Does this website appear to belong to the same company in the source row?

It is not an AI confidence score. It is not a Google rank. It is not a measure of whether the business is active, qualified, or worth suppressing. It is only a website-to-company identity score.

## How The Score Works

Each candidate website is scored from 0 to 100 using three evidence groups.
Company identity and location carry the decision. Domain similarity is
supporting evidence because real company domains often use acronyms,
abbreviations, or legacy brand names.

### 1. Company Match

Magpie compares the source company name to the candidate business name.

Examples of candidate names:

- Google Maps listing name
- Perplexity returned official name
- Manually verified known association name
- Salesforce or legacy database company/account name
- Website evidence name when available

Strong evidence:

- Exact or near-exact company name
- Clear DBA/name variant
- Distinctive words match, not just generic words

Weak evidence:

- Different company name
- Only one shared generic word
- Name is missing entirely

### 2. Domain Match

Magpie compares the website domain to the source company name. This is useful
supporting evidence, not the strongest signal by itself.

Strong evidence:

- Domain contains the company name or distinctive abbreviation
- Domain uses a detectable acronym or initials from the business name
- Domain clearly maps to the business name

Weak evidence:

- Domain has no relationship to the company name
- Domain only shares a generic word
- Domain belongs to a directory, social profile, marketplace, or unrelated brand

### 3. Location Match

Magpie compares expected location to evidence from Maps and search results.

Strong evidence:

- Same city and state
- Same state plus a business name/domain match
- Maps listing address aligns with the source row

Weak or negative evidence:

- No location evidence
- Different city with no other strong identity signal
- Different state, which is treated as a serious contradiction

## Score Interpretation

| Score / Tier | Meaning | Recommended Use |
|---|---|---|
| High | The deterministic identity verdict is `accepted`. | Usually usable, but review high-impact rows. |
| Medium | The deterministic identity verdict is `review` and location does not contradict the source row. | Human review recommended. |
| Low | Weak, missing, or contradictory evidence. | Treat as unresolved unless manually confirmed. |
| No candidate | No acceptable website was found. | Prefer blank over a wrong URL. |

The score itself is calculated from identity evidence, not from a provider's
stated confidence. A review candidate with a location contradiction is not
promoted to `final_url`. Directory, social-profile, and provider-profile URLs
are not treated as acceptable final websites unless they are manually verified.

## Important Rule

A missing website is not automatically a failure. For this workflow, returning no URL is better than returning a confident but wrong URL.

## What AI Does

Perplexity can help find or verify possible URLs using web search.

Haiku can inspect website text and answer factual validation questions when enabled.

Neither model should be treated as the source of truth for the final confidence score. AI output is evidence. Magpie's deterministic scorer decides how much that evidence matters.

## Known URL Associations

Known URL associations are manually researched company-location-to-website links.
They are local, editable, and auditable from the app's `Known URLs` page.
Deleting a known URL marks it inactive so old manual decisions remain traceable.

Manual associations are matched by company, city, state, and country. This is
intentional: a short name such as `ADS, LLC` should not automatically apply to
other ADS entities in other locations.

Manual URLs are treated as candidate evidence and still pass through the
deterministic identity scorer. When accepted, their `url_source` is
`manual_verified`.

## Historical Enrichment

Salesforce and legacy database enrichment are optional per job and read-only. They are treated as candidate sources, not final authorities.

When enabled, Magpie searches for prior records that line up with the source company, city, and state. Returned websites are scored by the same deterministic identity rules as Maps and Perplexity. A historical URL only becomes `final_url` when it clears the historical minimum score.

Legacy DB enrichment has a default read-only query for
`CustomerDB.dbo.tblCustomer`. It searches same-state customer rows by source
name terms and derives candidate URLs from contact email domains in `Email` and
`Email2`. Generic consumer domains such as Gmail, Yahoo, Outlook, and iCloud are
ignored. ISP-style contact domains such as `wildblue.net` are also ignored
because they identify the email provider rather than the customer. These
candidates are less trusted than live sources and must still pass the
deterministic historical scoring threshold before becoming `final_url`.

Legacy matching avoids broad business words such as `equipment` as the primary
SQL name term when a more distinctive company token is available. A legacy
email-domain candidate also needs distinctive source-name overlap with the
legacy customer name before it can clear final acceptance.

You can override the default with `MAGPIE_LEGACY_ENRICHMENT_QUERY` in `.env`.
The query must be `SELECT` or a read-only CTE and may use named parameters:
`:company`, `:city`, `:state`, `:country`, and `:name_like`.

## Key Output Columns

| Column | Meaning |
|---|---|
| `company`, `city`, `state`, `country` | Source identity from the uploaded file. |
| `gmaps_listing_name` | Business name returned by Google Maps. |
| `gmaps_website` | Website URL returned by Google Maps. |
| `perplexity_url` | Website candidate returned by Perplexity, when enabled. |
| `manual_url` | Manually verified URL candidate, when enabled and matched. |
| `manual_association_type` | Reason/type for the manually verified association. |
| `manual_notes` | Human-readable evidence note for the association. |
| `manual_identity_score` | Deterministic score for the manual candidate. |
| `historical_url` | Best Salesforce or legacy database candidate, when enabled. |
| `historical_source` | Historical source name: `salesforce` or `legacy_db`. |
| `historical_record_type` | Source object/table label for the historical candidate. |
| `historical_record_id` | Source record id when available. |
| `historical_raw_source` | Source detail, such as a Salesforce field or legacy email domain. |
| `historical_candidate_count` | Candidate URLs evaluated from historical sources for this row. |
| `historical_legacy_raw_rows` | Raw legacy SQL rows returned before email-domain filtering. |
| `historical_legacy_rows_with_email` | Legacy rows containing at least one email address. |
| `historical_legacy_usable_domains` | Non-generic email domains converted to candidate URLs. |
| `historical_legacy_filtered_domains` | Generic or invalid email domains ignored before scoring. |
| `historical_legacy_query_name_like` | Name-search parameter used for the legacy SQL lookup. |
| `historical_legacy_query_state*` | State values used for legacy lookup, including full name and abbreviation. |
| `historical_identity_score` | Deterministic score for the historical candidate. |
| `historical_errors` | Read-only source lookup errors, if the source was enabled but unavailable. |
| `final_url` | Selected candidate URL. |
| `url_source` | Source of the selected URL: Maps, Perplexity, manual verified, Salesforce, or legacy DB. |
| `final_confidence_score` | Deterministic 0-100 identity score for the selected URL; `0` when no URL is selected. |
| `confidence_tier` | High for accepted final URLs, Medium for review final URLs, blank for unresolved rows. |
| `identity_verdict` | Scorer verdict for the selected candidate. |
| `identity_reason` | Short explanation of the score inputs. |
| `company_match_score` | Name match component. |
| `domain_match_score` | Domain match component. |
| `location_match_level` | Location evidence summary. |
| `gmaps_identity_score` | Deterministic score for the Maps candidate. |
| `perplexity_identity_score` | Deterministic score for the Perplexity candidate. |

## App Flow

```text
Upload file
  -> Ingest and cleanse rows
  -> Drop rows missing required identity fields
  -> Search Google Maps
  -> Optionally check known URL associations
  -> Optionally check Salesforce / legacy DB read-only evidence
  -> Optionally ask Haiku to inspect Maps URL
  -> Optionally ask Perplexity to find or verify URL
  -> Score each candidate URL deterministically
  -> Pick the strongest candidate that clears the threshold
  -> Export results, evidence, and costs
```

## Drop Reasons

Rows can be dropped before evaluation when the app cannot build a usable source identity.

Common drop reasons:

- `missing_company`
- `missing_city`
- `missing_state`
- sheet or column structure could not be detected

The upload screen shows counts by reason and sample rows for each reason.

## Running The App

Install dependencies and start the local server from this folder:

```powershell
pip install -r requirements.txt
python run.py
```

Then open the local URL printed by the server.

## Cost Notes

Google Maps scraping does not use the paid Maps API.

Perplexity and Haiku calls may incur API costs when enabled. The app records call counts, token usage, and cost estimates in the run report.

## Current Design Priorities

- Favor correct abstention over false positives.
- Keep source data separate from appended evidence.
- Avoid multi-value cells in outputs.
- Make scoring deterministic and reviewable.
- Treat provider confidence as metadata, not ground truth.
