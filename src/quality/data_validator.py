#!/usr/bin/env python3
"""
Cloud FinOps Data Quality Validator

Runs automated data quality gate checks against the billing data pipeline
outputs. Designed to run in CI/CD as a pass/fail gate before anomaly detection.

Usage:
    python -m src.quality.data_validator

Exit Codes:
    0 - All quality checks passed
    1 - One or more quality checks failed
"""

import os
import sys
import logging
import glob
from dataclasses import dataclass, field

import duckdb

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RAW_PATH = "data/raw/cloud_billing_raw.parquet"
LAKE_PATH = "data/processed/lake"
MIN_RECORD_COUNT = 20_000
EXPECTED_SERVICES = {
    "SRV_EC2",
    "SRV_S3",
    "SRV_RDS",
    "SRV_SNOWFLAKE",
    "SRV_NAT",
    "SRV_EKS",
}
EXPECTED_MIN_PARTITIONS = 2

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    """Outcome of a single data-quality check."""

    check_name: str
    passed: bool
    message: str
    details: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------

class DataValidator:
    """Runs data-quality checks against raw parquet and partitioned lake."""

    def __init__(self, raw_path: str, lake_path: str) -> None:
        self.raw_path = raw_path
        self.lake_path = lake_path
        self.conn = duckdb.connect()

    def close(self) -> None:
        """Release the DuckDB connection."""
        self.conn.close()

    # -- individual checks ---------------------------------------------------

    def check_record_count(self) -> ValidationResult:
        """Verify the raw parquet contains at least MIN_RECORD_COUNT rows."""
        row_count = self.conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{self.raw_path}')"
        ).fetchone()[0]

        passed = row_count >= MIN_RECORD_COUNT
        return ValidationResult(
            check_name="Record Count",
            passed=passed,
            message=(
                f"{row_count:,} records found"
                if passed
                else f"Only {row_count:,} records (need >= {MIN_RECORD_COUNT:,})"
            ),
            details={"row_count": row_count},
        )

    def check_schema_integrity(self) -> ValidationResult:
        """Verify that all required columns exist in the raw parquet."""
        required_columns = [
            "record_id",
            "timestamp",
            "service_id",
            "service_name",
            "unblended_cost_usd",
            "resource_tags",
            "is_synthetic_anomaly",
        ]

        result = self.conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{self.raw_path}')"
        ).fetchall()
        actual_columns = {row[0] for row in result}

        missing = [c for c in required_columns if c not in actual_columns]
        passed = len(missing) == 0
        return ValidationResult(
            check_name="Schema Integrity",
            passed=passed,
            message=(
                f"All {len(required_columns)} required columns present"
                if passed
                else f"Missing columns: {', '.join(missing)}"
            ),
            details={"missing_columns": missing, "actual_columns": sorted(actual_columns)},
        )

    def check_null_values(self) -> ValidationResult:
        """Check for NULL values in critical columns."""
        critical_columns = [
            "record_id",
            "timestamp",
            "service_id",
            "unblended_cost_usd",
        ]

        null_counts: dict[str, int] = {}
        for col in critical_columns:
            count = self.conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{self.raw_path}') "
                f"WHERE {col} IS NULL"
            ).fetchone()[0]
            if count > 0:
                null_counts[col] = count

        passed = len(null_counts) == 0
        if passed:
            message = "No nulls in critical columns"
        else:
            parts = [f"{cnt} nulls in {col}" for col, cnt in null_counts.items()]
            message = "; ".join(parts)

        return ValidationResult(
            check_name="Null Values",
            passed=passed,
            message=message,
            details={"null_counts": null_counts},
        )

    def check_service_coverage(self) -> ValidationResult:
        """Verify all expected service_ids are present in the data."""
        rows = self.conn.execute(
            f"SELECT DISTINCT service_id FROM read_parquet('{self.raw_path}')"
        ).fetchall()
        actual_services = {row[0] for row in rows}

        missing = EXPECTED_SERVICES - actual_services
        passed = len(missing) == 0
        return ValidationResult(
            check_name="Service Coverage",
            passed=passed,
            message=(
                f"All {len(EXPECTED_SERVICES)} expected services present"
                if passed
                else f"Missing services: {', '.join(sorted(missing))}"
            ),
            details={
                "missing_services": sorted(missing),
                "actual_services": sorted(actual_services),
            },
        )

    def check_cost_sanity(self) -> ValidationResult:
        """Validate cost values fall within reasonable bounds."""
        row = self.conn.execute(
            f"SELECT MIN(unblended_cost_usd), MAX(unblended_cost_usd), "
            f"AVG(unblended_cost_usd) "
            f"FROM read_parquet('{self.raw_path}')"
        ).fetchone()

        min_cost, max_cost, avg_cost = row
        passed = min_cost >= 0 and max_cost < 50_000 and avg_cost > 0

        reasons: list[str] = []
        if min_cost < 0:
            reasons.append(f"negative costs detected (min={min_cost:.2f})")
        if max_cost >= 50_000:
            reasons.append(f"max cost too high ({max_cost:,.2f})")
        if avg_cost <= 0:
            reasons.append(f"avg cost non-positive ({avg_cost:.4f})")

        return ValidationResult(
            check_name="Cost Sanity",
            passed=passed,
            message=(
                f"Costs in range [${min_cost:.2f}, ${max_cost:,.2f}], avg ${avg_cost:.2f}"
                if passed
                else "; ".join(reasons)
            ),
            details={
                "min_cost": float(min_cost),
                "max_cost": float(max_cost),
                "avg_cost": float(avg_cost),
            },
        )

    def check_partition_integrity(self) -> ValidationResult:
        """Verify the partitioned lake has enough partitions and matching row counts."""
        # Discover partition parquet files in the lake
        pattern = os.path.join(self.lake_path, "**", "data_0.parquet")
        partition_files = glob.glob(pattern, recursive=True)

        if len(partition_files) < EXPECTED_MIN_PARTITIONS:
            return ValidationResult(
                check_name="Partition Integrity",
                passed=False,
                message=(
                    f"Only {len(partition_files)} partition(s) found "
                    f"(need >= {EXPECTED_MIN_PARTITIONS})"
                ),
                details={"partition_count": len(partition_files)},
            )

        # Total rows across partitioned files
        lake_count = 0
        for pf in partition_files:
            lake_count += self.conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{pf}')"
            ).fetchone()[0]

        raw_count = self.conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{self.raw_path}')"
        ).fetchone()[0]

        passed = lake_count == raw_count
        return ValidationResult(
            check_name="Partition Integrity",
            passed=passed,
            message=(
                f"{len(partition_files)} partitions, {lake_count:,} rows match raw"
                if passed
                else (
                    f"Row mismatch: lake={lake_count:,} vs raw={raw_count:,} "
                    f"({len(partition_files)} partitions)"
                )
            ),
            details={
                "partition_count": len(partition_files),
                "lake_row_count": lake_count,
                "raw_row_count": raw_count,
            },
        )

    def check_date_range(self) -> ValidationResult:
        """Verify that the data spans at least 30 days."""
        row = self.conn.execute(
            f"SELECT MIN(date), MAX(date) FROM read_parquet('{self.raw_path}')"
        ).fetchone()
        min_date, max_date = row
        span_days = (max_date - min_date).days

        passed = span_days >= 30
        return ValidationResult(
            check_name="Date Range",
            passed=passed,
            message=(
                f"Data spans {span_days} days ({min_date} to {max_date})"
                if passed
                else f"Only {span_days} days of data (need >= 30)"
            ),
            details={
                "min_date": str(min_date),
                "max_date": str(max_date),
                "span_days": span_days,
            },
        )

    def check_anomaly_flag_presence(self) -> ValidationResult:
        """Verify that injected anomaly flags are present in the data."""
        anomaly_count = self.conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{self.raw_path}') "
            f"WHERE is_synthetic_anomaly = TRUE"
        ).fetchone()[0]

        passed = anomaly_count > 0
        return ValidationResult(
            check_name="Anomaly Flag Presence",
            passed=passed,
            message=(
                f"{anomaly_count:,} anomaly-flagged records found"
                if passed
                else "No anomaly-flagged records found"
            ),
            details={"anomaly_count": anomaly_count},
        )


# ---------------------------------------------------------------------------
# Runner helpers
# ---------------------------------------------------------------------------

def run_all_checks(validator: DataValidator) -> list[ValidationResult]:
    """Execute every quality check, capturing exceptions as failures."""
    checks = [
        validator.check_record_count,
        validator.check_schema_integrity,
        validator.check_null_values,
        validator.check_service_coverage,
        validator.check_cost_sanity,
        validator.check_partition_integrity,
        validator.check_date_range,
        validator.check_anomaly_flag_presence,
    ]

    results: list[ValidationResult] = []
    for check_fn in checks:
        try:
            results.append(check_fn())
        except Exception as exc:  # noqa: BLE001
            logger.exception("Check '%s' raised an exception", check_fn.__name__)
            results.append(
                ValidationResult(
                    check_name=check_fn.__name__,
                    passed=False,
                    message=f"Exception: {exc}",
                    details={"exception": str(exc)},
                )
            )
    return results


def print_report(results: list[ValidationResult]) -> None:
    """Print a human-readable quality-gate report to stdout."""
    separator = "═" * 58
    print(f"\n{separator}")
    print("  CLOUD FINOPS DATA QUALITY REPORT")
    print(separator)

    for r in results:
        icon = "✅ PASS" if r.passed else "❌ FAIL"
        print(f"  {icon}  | {r.check_name:<22s} | {r.message}")

    passed_count = sum(1 for r in results if r.passed)
    total_count = len(results)
    print(separator)
    print(f"  RESULT: {passed_count}/{total_count} checks passed")
    print(f"{separator}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """CLI entry point for the data-quality validator."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    # Resolve paths relative to the project root (this file lives at
    # <project>/src/quality/data_validator.py → two levels up).
    base_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
    )
    raw_path = os.path.join(base_dir, RAW_PATH)
    lake_path = os.path.join(base_dir, LAKE_PATH)

    logger.info("Base directory : %s", base_dir)
    logger.info("Raw parquet    : %s", raw_path)
    logger.info("Lake directory : %s", lake_path)

    validator = DataValidator(raw_path=raw_path, lake_path=lake_path)
    try:
        results = run_all_checks(validator)
    finally:
        validator.close()

    print_report(results)

    if all(r.passed for r in results):
        logger.info("All quality checks passed ✅")
        sys.exit(0)
    else:
        failed = [r.check_name for r in results if not r.passed]
        logger.error("Quality gate FAILED ❌  — failed checks: %s", ", ".join(failed))
        sys.exit(1)


if __name__ == "__main__":
    main()
