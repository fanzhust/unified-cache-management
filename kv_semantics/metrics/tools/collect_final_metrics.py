#!/usr/bin/env python3
"""Capture and summarize the final snapshot from a Prometheus endpoint.

This script is intentionally independent of kv-test or any other workload. Start
it before the workload. It waits for the endpoint to appear, keeps only the most
recent successful snapshot, and writes a report when the endpoint disappears.

Example:
    python3 kv_semantics/metrics/tools/collect_final_metrics.py --label sync

For a reliable final snapshot, the observed process should flush metrics and keep
the endpoint alive briefly during shutdown. kv-test can use:

    metrics.shutdown_grace_ms=5000
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import re
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


DEFAULT_METRICS_URL = "http://127.0.0.1:9108/metrics"

COUNTERS = (
    "kv_client_batch_store_requests_total",
    "kv_client_batch_store_entries_total",
    "kv_client_batch_store_errors_total",
    "kv_client_wait_requests_total",
    "kv_client_wait_errors_total",
    "kv_transport_task_completion_timeouts_total",
    "kv_transport_task_io_timeouts_total",
    "kv_transport_task_connection_errors_total",
)

HISTOGRAMS = (
    "kv_client_task_enqueue_duration_seconds",
    "kv_client_task_queue_duration_seconds",
    "kv_client_task_process_duration_seconds",
    "kv_client_task_send_duration_seconds",
    "kv_client_task_e2e_duration_seconds",
    "kv_transport_task_pre_send_duration_seconds",
    "kv_transport_task_queue_duration_seconds",
    "kv_transport_task_process_duration_seconds",
    "kv_transport_task_send_call_duration_seconds",
    "kv_transport_task_send_duration_seconds",
    "kv_transport_task_completion_duration_seconds",
    "kv_transport_task_e2e_duration_seconds",
)

SAMPLE_RE = re.compile(
    r"^([A-Za-z_:][A-Za-z0-9_:]*)(?:\{(.*)\})?\s+"
    r"([-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][-+]?[0-9]+)?|"
    r"[+-]?[Ii]nf|[Nn]a[Nn])(?:\s+\d+)?$"
)
LABEL_RE = re.compile(r'([A-Za-z_][A-Za-z0-9_]*)="((?:\\.|[^"\\])*)"')


@dataclass(frozen=True)
class Sample:
    name: str
    labels: Mapping[str, str]
    value: float


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Wait for a Prometheus endpoint, retain its final snapshot, and summarize key "
            "KV client/transport metrics."
        )
    )
    parser.add_argument(
        "--label",
        required=True,
        help="Run label used in the artifact directory, for example sync or async.",
    )
    parser.add_argument(
        "--metrics-url",
        default=DEFAULT_METRICS_URL,
        help=f"Prometheus endpoint (default: {DEFAULT_METRICS_URL}).",
    )
    parser.add_argument(
        "--output-dir",
        default="./metrics-results",
        help="Base output directory (default: ./metrics-results).",
    )
    parser.add_argument(
        "--scrape-interval",
        type=float,
        default=1.0,
        help="Seconds between scrapes (default: 1.0).",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=1.0,
        help="HTTP request timeout in seconds (default: 1.0).",
    )
    parser.add_argument(
        "--startup-timeout",
        type=float,
        default=120.0,
        help="Seconds to wait for the endpoint to appear (default: 120).",
    )
    parser.add_argument(
        "--stop-after-failures",
        type=int,
        default=3,
        help="Stop after this many consecutive failures once scraping has started (default: 3).",
    )
    parser.add_argument(
        "--capture-seconds",
        type=float,
        default=0.0,
        help=(
            "Stop this many seconds after the first successful scrape; 0 waits for endpoint "
            "shutdown or Ctrl-C (default: 0)."
        ),
    )
    args = parser.parse_args(argv)
    if args.scrape_interval <= 0:
        parser.error("--scrape-interval must be greater than zero")
    if args.request_timeout <= 0:
        parser.error("--request-timeout must be greater than zero")
    if args.startup_timeout <= 0:
        parser.error("--startup-timeout must be greater than zero")
    if args.stop_after_failures <= 0:
        parser.error("--stop-after-failures must be greater than zero")
    if args.capture_seconds < 0:
        parser.error("--capture-seconds must not be negative")
    return args


def sanitize_label(label: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "_", label).strip("._-")
    return sanitized or "run"


def unescape_label(value: str) -> str:
    return value.replace(r"\n", "\n").replace(r'\"', '"').replace(r"\\", "\\")


def parse_labels(text: Optional[str]) -> Dict[str, str]:
    if not text:
        return {}
    return {match.group(1): unescape_label(match.group(2)) for match in LABEL_RE.finditer(text)}


def parse_prometheus(text: str) -> List[Sample]:
    samples: List[Sample] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = SAMPLE_RE.match(line)
        if not match:
            continue
        try:
            value = float(match.group(3))
        except ValueError:
            continue
        samples.append(Sample(match.group(1), parse_labels(match.group(2)), value))
    return samples


def values_for(samples: Iterable[Sample], name: str) -> List[Sample]:
    return [sample for sample in samples if sample.name == name]


def counter_value(samples: Iterable[Sample], name: str) -> float:
    return sum(sample.value for sample in values_for(samples, f"kv:{name}"))


def histogram_quantile(buckets: Sequence[Tuple[float, float]], count: float, q: float) -> float:
    if count <= 0 or not buckets:
        return math.nan
    rank = q * count
    previous_bound = 0.0
    previous_count = 0.0
    for upper_bound, cumulative_count in sorted(buckets, key=lambda item: item[0]):
        if cumulative_count < rank:
            if math.isfinite(upper_bound):
                previous_bound = upper_bound
            previous_count = cumulative_count
            continue
        if not math.isfinite(upper_bound):
            return previous_bound
        bucket_count = cumulative_count - previous_count
        if bucket_count <= 0:
            return upper_bound
        fraction = (rank - previous_count) / bucket_count
        return previous_bound + (upper_bound - previous_bound) * fraction
    return math.nan


def histogram_stats(samples: Iterable[Sample], base_name: str) -> Dict[str, float]:
    sample_list = list(samples)
    full_name = f"kv:{base_name}"
    total_sum = sum(sample.value for sample in values_for(sample_list, full_name + "_sum"))
    count = sum(sample.value for sample in values_for(sample_list, full_name + "_count"))
    bucket_totals: Dict[float, float] = {}
    for sample in values_for(sample_list, full_name + "_bucket"):
        le = sample.labels.get("le")
        if le is None:
            continue
        try:
            upper_bound = float(le)
        except ValueError:
            if le == "+Inf":
                upper_bound = math.inf
            else:
                continue
        bucket_totals[upper_bound] = bucket_totals.get(upper_bound, 0.0) + sample.value
    buckets = sorted(bucket_totals.items(), key=lambda item: item[0])
    result = {
        "count": count,
        "avg_us": total_sum / count * 1_000_000.0 if count > 0 else math.nan,
    }
    quantiles = (("p50_us", 0.50), ("p90_us", 0.90), ("p99_us", 0.99),
                 ("p99_9_us", 0.999))
    for percentile, quantile in quantiles:
        result[percentile] = histogram_quantile(buckets, count, quantile) * 1_000_000.0
    return result


def format_number(value: object) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        if not math.isfinite(value):
            return "n/a"
        return f"{value:.3f}"
    return str(value)


def json_safe(value: object) -> object:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return value


def build_report(
    label: str,
    metrics_url: str,
    scrape_count: int,
    first_scrape_at: dt.datetime,
    final_scrape_at: dt.datetime,
    stop_reason: str,
    metrics_text: str,
) -> Tuple[str, Dict[str, object]]:
    samples = parse_prometheus(metrics_text)
    counters = {name: counter_value(samples, name) for name in COUNTERS}
    histograms = {name: histogram_stats(samples, name) for name in HISTOGRAMS}
    observed_seconds = max(0.0, (final_scrape_at - first_scrape_at).total_seconds())

    result: Dict[str, object] = {
        "label": label,
        "metrics_url": metrics_url,
        "successful_scrapes": scrape_count,
        "first_scrape_at": first_scrape_at.isoformat(),
        "final_scrape_at": final_scrape_at.isoformat(),
        "observed_seconds": observed_seconds,
        "stop_reason": stop_reason,
        "counters": counters,
        "histograms": histograms,
    }

    lines = [
        f"# Metrics final report: {label}",
        "",
        f"- metrics_url: {metrics_url}",
        f"- successful_scrapes: {scrape_count}",
        f"- first_scrape_at: {first_scrape_at.isoformat()}",
        f"- final_scrape_at: {final_scrape_at.isoformat()}",
        f"- observed_seconds: {observed_seconds:.3f}",
        f"- stop_reason: {stop_reason}",
        "",
        "## Counters (process lifetime; includes warmup)",
        "",
        "| metric | value |",
        "| --- | ---: |",
    ]
    for name, value in counters.items():
        lines.append(f"| {name} | {format_number(value)} |")

    lines.extend([
        "",
        "## Duration histograms (process lifetime; bucket-interpolated percentiles)",
        "",
        "| metric | count | avg_us | p50_us | p90_us | p99_us | p99.9_us |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    for name, stats in histograms.items():
        lines.append(
            "| {} | {} | {} | {} | {} | {} | {} |".format(
                name,
                format_number(stats["count"]),
                format_number(stats["avg_us"]),
                format_number(stats["p50_us"]),
                format_number(stats["p90_us"]),
                format_number(stats["p99_us"]),
                format_number(stats["p99_9_us"]),
            )
        )

    lines.extend([
        "",
        "## Notes",
        "",
        "- The report describes the final cumulative process snapshot; it does not calculate",
        "  rates from the scrape interval.",
        "- Compare sync/async primarily with transport queue, send, completion, and e2e",
        "  durations plus the workload's own throughput and error summary.",
        "- Do not directly compare kv_transport_task_send_call_duration_seconds: sync measures",
        "  the synchronous Send call, while async measures only the AsyncSend launch stage.",
        "- Histogram percentiles are estimates within the configured bucket bounds.",
        "",
    ])
    return "\n".join(lines), result


def scrape(url: str, timeout: float) -> Optional[str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            if response.status != 200:
                return None
            return response.read().decode("utf-8")
    except (OSError, UnicodeDecodeError, urllib.error.URLError):
        return None


def save_snapshot(run_dir: Path, snapshot: str) -> None:
    temporary_path = run_dir / ".metrics.prom.tmp"
    temporary_path.write_text(snapshot, encoding="utf-8")
    temporary_path.replace(run_dir / "metrics.prom")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    label = sanitize_label(args.label)
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(args.output_dir).expanduser() / f"{label}_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=False)

    print(f"metrics artifacts: {run_dir.resolve()}")
    print(f"waiting for metrics endpoint: {args.metrics_url}")
    print("start the workload in another terminal; press Ctrl-C to finish manually")

    started_waiting_at = time.monotonic()
    first_scrape_at: Optional[dt.datetime] = None
    final_scrape_at: Optional[dt.datetime] = None
    final_metrics: Optional[str] = None
    scrape_count = 0
    consecutive_failures = 0
    stop_reason = "unknown"

    try:
        while True:
            snapshot = scrape(args.metrics_url, args.request_timeout)
            now = dt.datetime.now(dt.timezone.utc)
            if snapshot is not None:
                if first_scrape_at is None:
                    first_scrape_at = now
                    print(f"metrics endpoint is ready: {first_scrape_at.isoformat()}")
                final_metrics = snapshot
                final_scrape_at = now
                scrape_count += 1
                consecutive_failures = 0
                save_snapshot(run_dir, snapshot)
                if (args.capture_seconds > 0 and
                        (now - first_scrape_at).total_seconds() >= args.capture_seconds):
                    stop_reason = "capture duration reached"
                    break
            elif first_scrape_at is None:
                if time.monotonic() - started_waiting_at >= args.startup_timeout:
                    stop_reason = "startup timeout"
                    break
            else:
                consecutive_failures += 1
                if consecutive_failures >= args.stop_after_failures:
                    stop_reason = "metrics endpoint stopped"
                    break
            time.sleep(args.scrape_interval)
    except KeyboardInterrupt:
        stop_reason = "interrupted by user"
        print("\ncollection interrupted; analyzing the last successful snapshot")

    if final_metrics is None or first_scrape_at is None or final_scrape_at is None:
        message = "no metrics snapshot was collected"
        metadata = {
            "label": label,
            "metrics_url": args.metrics_url,
            "stop_reason": stop_reason,
            "error": message,
        }
        (run_dir / "report.json").write_text(
            json.dumps(metadata, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        print(f"error: {message}; stop_reason={stop_reason}", file=sys.stderr)
        return 2

    report_text, report_json = build_report(
        label,
        args.metrics_url,
        scrape_count,
        first_scrape_at,
        final_scrape_at,
        stop_reason,
        final_metrics,
    )
    (run_dir / "report.md").write_text(report_text, encoding="utf-8")
    (run_dir / "report.json").write_text(
        json.dumps(json_safe(report_json), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )

    print(report_text)
    print(f"report: {(run_dir / 'report.md').resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
