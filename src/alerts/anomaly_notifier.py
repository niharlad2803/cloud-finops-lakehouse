#!/usr/bin/env python3
"""
Cloud FinOps Anomaly Alerting Service

Queries the local DuckDB-powered data lakehouse to detect cost anomalies
using rolling 7-day 3-sigma statistical analysis, then generates structured
Slack incident alerts for flagged spend spikes.

Usage:
    python -m src.alerts.anomaly_notifier              # Live mode (requires SLACK_WEBHOOK_URL)
    python -m src.alerts.anomaly_notifier --dry-run    # Preview mode (stdout + JSON report)
"""

import os
import sys
import json
import logging
import argparse

import duckdb
import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LAKE_PATH = "data/processed/lake/**/*.parquet"
ANOMALY_REPORT_PATH = "data/anomaly_report.json"
SIGMA_THRESHOLD = 2.5
EXCESS_SPEND_THRESHOLD = 200

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Anomaly detection
# ---------------------------------------------------------------------------

ANOMALY_SQL = """
WITH daily_service_spend AS (
    SELECT
        CAST(timestamp AS DATE) AS date_day,
        service_id,
        ROUND(SUM(unblended_cost_usd), 2) AS total_daily_spend
    FROM read_parquet('{lake_path}')
    GROUP BY CAST(timestamp AS DATE), service_id
),
rolling_baseline AS (
    SELECT
        date_day,
        service_id,
        total_daily_spend,
        AVG(total_daily_spend) OVER (
            PARTITION BY service_id
            ORDER BY date_day
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS rolling_7d_avg,
        STDDEV(total_daily_spend) OVER (
            PARTITION BY service_id
            ORDER BY date_day
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS rolling_7d_stddev
    FROM daily_service_spend
)
SELECT
    date_day,
    service_id,
    total_daily_spend,
    ROUND(rolling_7d_avg, 2) AS rolling_7d_avg,
    ROUND(total_daily_spend - rolling_7d_avg, 2) AS excess_spend_usd,
    CASE
        WHEN rolling_7d_stddev IS NOT NULL AND rolling_7d_stddev > 0
            THEN ROUND((total_daily_spend - rolling_7d_avg) / rolling_7d_stddev, 2)
        ELSE 0
    END AS z_score,
    CASE
        WHEN rolling_7d_stddev IS NOT NULL
             AND (total_daily_spend - rolling_7d_avg) > ({sigma_threshold} * rolling_7d_stddev)
             AND (total_daily_spend - rolling_7d_avg) > {excess_spend_threshold}
            THEN TRUE
        ELSE FALSE
    END AS is_anomaly_alert
FROM rolling_baseline
ORDER BY date_day DESC, excess_spend_usd DESC;
"""


def run_anomaly_detection(lake_path: str) -> pd.DataFrame:
    """Execute rolling 7-day 3-sigma anomaly detection against the parquet lakehouse.

    Args:
        lake_path: Glob path to the partitioned Parquet files.

    Returns:
        DataFrame containing all rows with anomaly scoring columns.
    """
    query = ANOMALY_SQL.format(
        lake_path=lake_path,
        sigma_threshold=SIGMA_THRESHOLD,
        excess_spend_threshold=EXCESS_SPEND_THRESHOLD,
    )
    logger.info("Connecting to DuckDB (in-memory) and scanning parquet lake: %s", lake_path)
    con = duckdb.connect(database=":memory:")
    try:
        df = con.execute(query).fetchdf()
        logger.info("Anomaly detection complete – %d rows evaluated.", len(df))
        return df
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Filtering & enrichment
# ---------------------------------------------------------------------------


def filter_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """Filter the scored DataFrame to anomaly-flagged rows and assign severity.

    Args:
        df: Full anomaly-scored DataFrame from :func:`run_anomaly_detection`.

    Returns:
        DataFrame containing only anomaly rows with an added ``severity`` column.
    """
    anomalies = df[df["is_anomaly_alert"] == True].copy()  # noqa: E712
    anomalies["severity"] = anomalies["z_score"].apply(
        lambda z: "CRITICAL" if z > 4.0 else "WARNING"
    )
    logger.info("Filtered to %d anomalies (CRITICAL: %d, WARNING: %d).",
                len(anomalies),
                (anomalies["severity"] == "CRITICAL").sum(),
                (anomalies["severity"] == "WARNING").sum())
    return anomalies


# ---------------------------------------------------------------------------
# Slack formatting
# ---------------------------------------------------------------------------


def format_slack_alert(anomalies_df: pd.DataFrame) -> dict:
    """Build a Slack Block Kit payload from the anomalies DataFrame.

    Args:
        anomalies_df: DataFrame of anomaly rows (must include ``severity``).

    Returns:
        A dict representing the Slack Block Kit message payload.
    """
    count = len(anomalies_df)
    blocks: list[dict] = []

    # Header
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f":rotating_light: FinOps Cost Anomaly Alert — {count} detected",
            "emoji": True,
        },
    })

    # Context – timestamp
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"*Generated:* {pd.Timestamp.now(tz='UTC').strftime('%Y-%m-%d %H:%M:%S UTC')}",
            }
        ],
    })

    blocks.append({"type": "divider"})

    # Individual anomaly sections (cap at 10)
    display_rows = anomalies_df.head(10)
    for _, row in display_rows.iterrows():
        emoji = "\U0001f534" if row["severity"] == "CRITICAL" else "\U0001f7e1"  # 🔴 / 🟡
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{emoji} *{row['severity']}* — `{row['service_id']}`\n"
                    f"• *Date:* {row['date_day']}\n"
                    f"• *Daily Spend:* ${row['total_daily_spend']:,.2f} vs "
                    f"*Rolling Avg:* ${row['rolling_7d_avg']:,.2f}\n"
                    f"• *Z-Score:* {row['z_score']:.2f} | "
                    f"*Excess Spend:* ${row['excess_spend_usd']:,.2f}"
                ),
            },
        })

    # Overflow notice
    remaining = count - 10
    if remaining > 0:
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"... and *{remaining}* more anomalies",
                }
            ],
        })

    blocks.append({"type": "divider"})

    return {"blocks": blocks}


# ---------------------------------------------------------------------------
# Slack delivery
# ---------------------------------------------------------------------------


def send_slack_alert(payload: dict, webhook_url: str) -> bool:
    """POST a Block Kit payload to a Slack incoming webhook.

    Args:
        payload: Slack Block Kit message dict.
        webhook_url: Slack incoming webhook URL.

    Returns:
        True if the request succeeded (HTTP 2xx), False otherwise.
    """
    try:
        response = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        if response.ok:
            logger.info("Slack alert sent successfully (HTTP %d).", response.status_code)
            return True
        else:
            logger.error("Slack alert failed (HTTP %d): %s", response.status_code, response.text)
            return False
    except requests.RequestException as exc:
        logger.error("Slack alert delivery error: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Report persistence
# ---------------------------------------------------------------------------


def save_report(anomalies_df: pd.DataFrame, output_path: str) -> None:
    """Persist the anomalies DataFrame as a JSON report.

    Args:
        anomalies_df: DataFrame of anomaly rows.
        output_path: Destination file path for the JSON report.
    """
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    anomalies_df.to_json(output_path, orient="records", date_format="iso")
    logger.info("Anomaly report saved to %s (%d records).", output_path, len(anomalies_df))


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Parse arguments and orchestrate anomaly detection → alerting pipeline."""
    parser = argparse.ArgumentParser(
        description="Cloud FinOps Anomaly Alerting Service",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview anomalies on stdout and write JSON report without sending Slack alerts.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    # Resolve paths relative to project root
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    lake_path = os.path.join(base_dir, LAKE_PATH)
    report_path = os.path.join(base_dir, ANOMALY_REPORT_PATH)

    # --- Detection -----------------------------------------------------------
    df = run_anomaly_detection(lake_path)
    anomalies = filter_anomalies(df)

    if anomalies.empty:
        logger.info("No cost anomalies detected. Exiting.")
        sys.exit(0)

    # --- Formatting & persistence --------------------------------------------
    slack_payload = format_slack_alert(anomalies)
    save_report(anomalies, report_path)

    if args.dry_run:
        logger.info("=== DRY-RUN MODE – Slack alert payload ===")
        logger.info(json.dumps(slack_payload, indent=2))
        logger.info("Dry-run complete. %d anomalies written to %s", len(anomalies), report_path)
        sys.exit(0)

    # --- Live delivery -------------------------------------------------------
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        logger.error("SLACK_WEBHOOK_URL environment variable is not set. Cannot send alert.")
        sys.exit(1)

    success = send_slack_alert(slack_payload, webhook_url)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
