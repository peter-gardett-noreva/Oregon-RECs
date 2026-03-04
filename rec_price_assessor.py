"""
Oregon Book & Claim REC Price Assessment Tool
Supports multi-source ingestion, normalization, and price estimation.
"""

import json
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
from statistics import mean, stdev

# ── Data Model ────────────────────────────────────────────────────────────────

@dataclass
class RECRecord:
    """Represents a single REC price data point."""
    source: str               # e.g. "broker_x", "exchange_y", "manual"
    price_usd: float          # price per MWh
    vintage_year: int         # year of generation
    technology: str           # "wind", "solar", "hydro", "geothermal", etc.
    region: str               # e.g. "PNW", "WECC", "Oregon"
    transaction_date: str     # ISO date string
    volume_mwh: Optional[float] = None
    certified: bool = True    # Green-e or equivalent
    notes: str = ""

@dataclass
class AssessmentResult:
    """Output of the price assessment."""
    timestamp: str
    technology: str
    vintage_year: int
    region: str
    sample_count: int
    mean_price: float
    min_price: float
    max_price: float
    std_dev: Optional[float]
    weighted_avg: Optional[float]   # volume-weighted if volumes available
    confidence: str                 # "high", "medium", "low"
    adjustments_applied: list[str] = field(default_factory=list)
    final_assessed_price: float = 0.0

# ── Adjustment Factors ────────────────────────────────────────────────────────

VINTAGE_DISCOUNT = {   # discount per year of age relative to current year
    0: 0.00,
    1: 0.03,
    2: 0.07,
    3: 0.12,
    4: 0.18,
    5: 0.25,
}

TECH_PREMIUM = {       # relative premium/discount by technology
    "wind":       0.00,
    "solar":      0.05,
    "geothermal": 0.08,
    "hydro":      -0.05,  # large hydro often trades at a discount
    "biomass":    -0.03,
}

CERTIFICATION_PREMIUM = 0.10   # Green-e or equivalent adds ~10%

# ── Core Assessment Engine ────────────────────────────────────────────────────

class OregonRECPriceAssessor:
    def __init__(self):
        self.records: list[RECRecord] = []

    # -- Data Ingestion --------------------------------------------------------

    def add_record(self, record: RECRecord):
        self.records.append(record)

    def load_from_json(self, json_str: str):
        """Load records from a JSON string (broker feed, API response, etc.)."""
        data = json.loads(json_str)
        for item in data:
            self.records.append(RECRecord(**item))

    def load_from_csv_row(self, row: dict):
        """Ingest a single row from a CSV/DataFrame."""
        self.records.append(RECRecord(
            source=row.get("source", "csv"),
            price_usd=float(row["price_usd"]),
            vintage_year=int(row["vintage_year"]),
            technology=row.get("technology", "wind").lower(),
            region=row.get("region", "Oregon"),
            transaction_date=row.get("transaction_date", datetime.today().isoformat()),
            volume_mwh=float(row["volume_mwh"]) if row.get("volume_mwh") else None,
            certified=str(row.get("certified", "true")).lower() == "true",
            notes=row.get("notes", ""),
        ))

    # -- Filtering -------------------------------------------------------------

    def filter_records(
        self,
        technology: Optional[str] = None,
        vintage_year: Optional[int] = None,
        region: Optional[str] = None,
        certified_only: bool = False,
    ) -> list[RECRecord]:
        results = self.records
        if technology:
            results = [r for r in results if r.technology == technology.lower()]
        if vintage_year:
            results = [r for r in results if r.vintage_year == vintage_year]
        if region:
            results = [r for r in results if region.lower() in r.region.lower()]
        if certified_only:
            results = [r for r in results if r.certified]
        return results

    # -- Price Assessment ------------------------------------------------------

    def assess(
        self,
        technology: str = "wind",
        vintage_year: Optional[int] = None,
        region: str = "Oregon",
        certified_only: bool = True,
        apply_vintage_adj: bool = True,
        apply_tech_adj: bool = True,
        apply_cert_adj: bool = True,
    ) -> AssessmentResult:

        vintage_year = vintage_year or datetime.today().year
        records = self.filter_records(technology, vintage_year, region, certified_only)

        if not records:
            raise ValueError(
                f"No records found for {technology} / {vintage_year} / {region}"
            )

        prices = [r.price_usd for r in records]
        volumes = [r.volume_mwh for r in records if r.volume_mwh]

        # Volume-weighted average (if volumes present)
        weighted_avg = None
        if len(volumes) == len(records):
            total_vol = sum(volumes)
            weighted_avg = sum(r.price_usd * r.volume_mwh for r in records) / total_vol

        base_price = weighted_avg if weighted_avg else mean(prices)
        adjustments = []

        # Vintage adjustment
        age = datetime.today().year - vintage_year
        if apply_vintage_adj and age > 0:
            discount = VINTAGE_DISCOUNT.get(min(age, 5), 0.25)
            base_price *= (1 - discount)
            adjustments.append(f"Vintage discount ({age}yr): -{discount*100:.0f}%")

        # Technology adjustment
        if apply_tech_adj and technology in TECH_PREMIUM:
            adj = TECH_PREMIUM[technology]
            base_price *= (1 + adj)
            sign = "+" if adj >= 0 else ""
            adjustments.append(f"Tech premium ({technology}): {sign}{adj*100:.0f}%")

        # Certification premium
        if apply_cert_adj and certified_only:
            base_price *= (1 + CERTIFICATION_PREMIUM)
            adjustments.append(f"Certification premium: +{CERTIFICATION_PREMIUM*100:.0f}%")

        # Confidence tier
        n = len(records)
        confidence = "high" if n >= 10 else "medium" if n >= 4 else "low"

        result = AssessmentResult(
            timestamp=datetime.now().isoformat(),
            technology=technology,
            vintage_year=vintage_year,
            region=region,
            sample_count=n,
            mean_price=mean(prices),
            min_price=min(prices),
            max_price=max(prices),
            std_dev=stdev(prices) if n > 1 else None,
            weighted_avg=weighted_avg,
            confidence=confidence,
            adjustments_applied=adjustments,
            final_assessed_price=round(base_price, 4),
        )
        return result

    # -- Reporting -------------------------------------------------------------

    def print_report(self, result: AssessmentResult):
        print("=" * 55)
        print("  Oregon Book & Claim REC — Price Assessment Report")
        print("=" * 55)
        print(f"  As of        : {result.timestamp[:10]}")
        print(f"  Technology   : {result.technology.title()}")
        print(f"  Vintage Year : {result.vintage_year}")
        print(f"  Region       : {result.region}")
        print(f"  Samples      : {result.sample_count}  (confidence: {result.confidence})")
        print("-" * 55)
        print(f"  Mean Price   : ${result.mean_price:.4f}/MWh")
        print(f"  Range        : ${result.min_price:.4f} – ${result.max_price:.4f}/MWh")
        if result.std_dev:
            print(f"  Std Dev      : ${result.std_dev:.4f}")
        if result.weighted_avg:
            print(f"  Vol-Wtd Avg  : ${result.weighted_avg:.4f}/MWh")
        print("-" * 55)
        print("  Adjustments Applied:")
        for adj in result.adjustments_applied:
            print(f"    • {adj}")
        print("-" * 55)
        print(f"  ASSESSED PRICE: ${result.final_assessed_price:.4f} / MWh")
        print("=" * 55)


# ── Example Usage ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    assessor = OregonRECPriceAssessor()

    # Simulate broker / exchange data
    sample_data = [
        RECRecord("broker_a",  2.50, 2024, "wind",  "Oregon", "2025-01-10", 5000,  True),
        RECRecord("broker_b",  2.65, 2024, "wind",  "Oregon", "2025-01-15", 3000,  True),
        RECRecord("exchange_x",2.45, 2024, "wind",  "PNW",    "2025-01-20", 10000, True),
        RECRecord("broker_a",  2.70, 2024, "wind",  "Oregon", "2025-02-01", 2000,  True),
        RECRecord("broker_c",  2.55, 2024, "wind",  "Oregon", "2025-02-10", 4500,  True),
    ]
    for rec in sample_data:
        assessor.add_record(rec)

    result = assessor.assess(
        technology="wind",
        vintage_year=2024,
        region="Oregon",
        certified_only=True,
        apply_vintage_adj=True,
    )
    assessor.print_report(result)
