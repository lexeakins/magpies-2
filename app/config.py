import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[3] / ".env")
load_dotenv(Path(__file__).parent.parent / ".env")


DEFAULT_LEGACY_ENRICHMENT_QUERY = """
SELECT TOP 25
       CustomerID AS record_id,
       CustomerName AS company_name,
       c_Company AS alt_company_name,
       ParentCompany AS parent_company,
       ShipName AS ship_name,
       BillCity AS city,
       BillState AS state,
       BillCountry AS country,
       ShipCity AS ship_city,
       ShipState AS ship_state,
       ShipCountry AS ship_country,
       Email,
       Email2,
       LastModifiedDate AS last_modified_date,
       'CustomerDB.dbo.tblCustomer' AS record_type,
       'CustomerDB.dbo.tblCustomer Email' AS raw_source
FROM [CustomerDB].[dbo].[tblCustomer]
WHERE (BillState IN (:state, :state_abbr, :state_full)
       OR ShipState IN (:state, :state_abbr, :state_full))
  AND (
        CustomerName LIKE :name_like
        OR c_Company LIKE :name_like
        OR ParentCompany LIKE :name_like
        OR ShipName LIKE :name_like
      )
  AND (Email LIKE '%@%' OR Email2 LIKE '%@%')
ORDER BY LastModifiedDate DESC
""".strip()


class _Settings:
    def __init__(self):
        # ── API keys ────────────────────────────────────────────────────
        self.anthropic_api_key:  str = os.getenv("ANTHROPIC_API_KEY", "")
        self.perplexity_api_key: str = os.getenv("PERPLEXITY_API_KEY", "")

        # ── Pipeline toggles ────────────────────────────────────────────
        self.haiku_enabled:              bool = True
        self.haiku_validation_enabled:   bool = True   # post-Perplexity Haiku pass
        self.perplexity_enabled:         bool = False
        self.perplexity_validate:        bool = True   # use Perplexity to verify existing URLs too
        # When does Perplexity fire?
        #   "no_url_or_low_confidence"  — Maps found no URL, OR Haiku said low confidence
        #   "no_url_only"               — only when Maps found no URL
        #   "always"                    — every record
        self.perplexity_trigger:         str  = "no_url_or_low_confidence"
        self.manual_associations_enabled:    bool = True
        self.salesforce_enrichment_enabled: bool = False
        self.legacy_enrichment_enabled:     bool = False
        self.web_search_fallback_enabled:   bool = False
        self.bing_web_enabled:              bool = True
        self.duckduckgo_web_enabled:        bool = True
        self.bing_maps_enabled:             bool = False

        # ── Confidence thresholds ────────────────────────────────────────
        self.high_confidence_threshold:  int  = 70
        self.low_confidence_threshold:   int  = 40
        self.haiku_confidence_threshold: int  = 65   # below this, Haiku passes to Perplexity

        # ── Retry (Maps errors only — not low confidence) ────────────────
        self.max_scrape_errors:          int  = 2    # retry Maps on genuine errors
        self.gmaps_max_candidates_per_mode: int = 5
        self.gmaps_strong_stop_score:       int = 90
        self.web_search_max_results:        int = 10

        # ── Workers ─────────────────────────────────────────────────────
        self.scrape_workers:   int = 4
        self.validate_workers: int = 4
        self.job_stall_timeout_seconds: int = int(os.getenv("MAGPIE_JOB_STALL_TIMEOUT_SECONDS", "1800"))

        # ── Timeouts ────────────────────────────────────────────────────
        self.jina_timeout_seconds: int = 30

        # ── Anthropic / Haiku ────────────────────────────────────────────
        self.haiku_model:                   str   = "claude-haiku-4-5-20251001"
        self.haiku_input_price_per_million: float = 0.80
        self.haiku_output_price_per_million:float = 4.00

        # ── Perplexity ───────────────────────────────────────────────────
        self.perplexity_model:                   str   = "sonar"
        self.perplexity_input_price_per_million: float = 1.00
        self.perplexity_output_price_per_million:float = 1.00
        self.perplexity_search_fee_per_request:  float = 0.005

        # ── Historical enrichment sources ─────────────────────────────
        # Optional, read-only sources used as candidate website evidence.
        self.historical_enrichment_min_score: int = 85
        self.salesforce_enrichment_objects:   str = "Account,Lead,Contact"
        self.legacy_enrichment_query:         str = os.getenv("MAGPIE_LEGACY_ENRICHMENT_QUERY") or DEFAULT_LEGACY_ENRICHMENT_QUERY
        self.legacy_enrichment_source_label:  str = os.getenv("MAGPIE_LEGACY_ENRICHMENT_SOURCE_LABEL", "legacy_db")

    # ── Frontend-safe export ─────────────────────────────────────────────

    def to_dict(self) -> dict:
        """All settings safe to expose to the UI (no API keys)."""
        return {
            # Pipeline
            "haiku_enabled":              self.haiku_enabled,
            "haiku_validation_enabled":   self.haiku_validation_enabled,
            "perplexity_enabled":         self.perplexity_enabled,
            "perplexity_validate":        self.perplexity_validate,
            "perplexity_trigger":         self.perplexity_trigger,
            "manual_associations_enabled": self.manual_associations_enabled,
            "salesforce_enrichment_enabled": self.salesforce_enrichment_enabled,
            "legacy_enrichment_enabled":     self.legacy_enrichment_enabled,
            "web_search_fallback_enabled":   self.web_search_fallback_enabled,
            "bing_web_enabled":              self.bing_web_enabled,
            "duckduckgo_web_enabled":        self.duckduckgo_web_enabled,
            "bing_maps_enabled":             self.bing_maps_enabled,
            # Thresholds
            "high_confidence_threshold":  self.high_confidence_threshold,
            "low_confidence_threshold":   self.low_confidence_threshold,
            "haiku_confidence_threshold": self.haiku_confidence_threshold,
            # Workers / timeouts
            "scrape_workers":             self.scrape_workers,
            "validate_workers":           self.validate_workers,
            "job_stall_timeout_seconds":  self.job_stall_timeout_seconds,
            "jina_timeout_seconds":       self.jina_timeout_seconds,
            "max_scrape_errors":          self.max_scrape_errors,
            "gmaps_max_candidates_per_mode": self.gmaps_max_candidates_per_mode,
            "gmaps_strong_stop_score":       self.gmaps_strong_stop_score,
            "web_search_max_results":        self.web_search_max_results,
            # Models
            "haiku_model":                self.haiku_model,
            "perplexity_model":           self.perplexity_model,
            # Pricing (display only)
            "haiku_input_price_per_million":          self.haiku_input_price_per_million,
            "haiku_output_price_per_million":         self.haiku_output_price_per_million,
            "perplexity_input_price_per_million":     self.perplexity_input_price_per_million,
            "perplexity_output_price_per_million":    self.perplexity_output_price_per_million,
            "perplexity_search_fee_per_request":      self.perplexity_search_fee_per_request,
            "historical_enrichment_min_score":         self.historical_enrichment_min_score,
            "salesforce_enrichment_objects":           self.salesforce_enrichment_objects,
            "legacy_enrichment_source_label":          self.legacy_enrichment_source_label,
            "legacy_enrichment_configured":            self._legacy_configured(),
            "salesforce_enrichment_configured":        self._salesforce_configured(),
        }

    def update(self, data: dict):
        """Update settings from the UI settings page."""
        int_keys   = {"high_confidence_threshold", "low_confidence_threshold",
                      "haiku_confidence_threshold", "scrape_workers", "validate_workers",
                      "job_stall_timeout_seconds", "jina_timeout_seconds", "max_scrape_errors",
                      "historical_enrichment_min_score",
                      "gmaps_max_candidates_per_mode", "gmaps_strong_stop_score",
                      "web_search_max_results"}
        float_keys = {"haiku_input_price_per_million", "haiku_output_price_per_million",
                      "perplexity_input_price_per_million", "perplexity_output_price_per_million",
                      "perplexity_search_fee_per_request"}
        bool_keys  = {"haiku_enabled", "haiku_validation_enabled", "perplexity_enabled",
                      "perplexity_validate", "manual_associations_enabled",
                      "salesforce_enrichment_enabled",
                      "legacy_enrichment_enabled",
                      "web_search_fallback_enabled", "bing_web_enabled",
                      "duckduckgo_web_enabled", "bing_maps_enabled"}
        str_keys   = {"perplexity_trigger", "haiku_model", "perplexity_model",
                      "salesforce_enrichment_objects", "legacy_enrichment_source_label"}

        for key, value in data.items():
            if key in int_keys:
                setattr(self, key, int(value))
            elif key in float_keys:
                setattr(self, key, float(value))
            elif key in bool_keys:
                setattr(self, key, bool(value))
            elif key in str_keys:
                setattr(self, key, str(value))

    def pipeline_snapshot(self) -> dict:
        """Exact pipeline state for experiment reproducibility."""
        return self.to_dict()

    def _salesforce_configured(self) -> bool:
        prefix = os.getenv("SF_ENV_PREFIX", "SF").strip()

        def sf_env(key: str) -> str:
            if prefix and prefix != "SF":
                return os.getenv(f"{prefix}_{key[3:]}") or os.getenv(key, "")
            return os.getenv(key, "")

        return bool(sf_env("SF_USERNAME") and sf_env("SF_PASSWORD") and sf_env("SF_SECURITY_TOKEN"))

    def _legacy_configured(self) -> bool:
        if not self.legacy_enrichment_query:
            return False
        return bool(os.getenv("DB_HOST") and os.getenv("DB_NAME"))


settings = _Settings()
