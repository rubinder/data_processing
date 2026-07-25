"""Render the benchmark results as an SVG that GitHub displays inline.

The result tables are precise but not immediate: someone landing on this folder
should see the shape of the tuning curve before reading a number. This turns
``results/latest.json`` into ``results/tuning.svg``.

Deliberately dependency-free (no matplotlib) so it regenerates as part of the
benchmark run rather than needing a plotting stack.

Colours are chosen to read on both the light and dark GitHub themes: GitHub
serves images from a sandboxed domain, so an SVG cannot inherit page styles or
respond to ``prefers-color-scheme``. Everything therefore uses mid-tone fills
on a transparent background, which has adequate contrast either way.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from realtime_analytics.queries import QUERY_TITLES  # noqa: E402

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

# Mid-tones: legible on white and on #0d1117 alike.
COLOR_SLOW = "#e15759"     # the naive baseline
COLOR_MID = "#4e79a7"      # intermediate stages
COLOR_FAST = "#59a14f"     # the shipped best
COLOR_TEXT = "#8b949e"
COLOR_LABEL = "#768390"

WIDTH = 900
ROW_H = 26
BAR_H = 15
LEFT = 210          # stage labels
RIGHT = 300         # value + speedup + rows labels
GROUP_GAP = 46


#: The benchmark's own stage labels are full sentences and get clipped.
SHORT_LABELS = {
    "v0_naive": "naive (String + JSON)",
    "v1_typed": "+ types & codecs",
    "v2_sorted": "+ sorting key",
    "v3_partitioned": "+ partitioning",
    "v4_indexed": "+ skipping indexes",
    "v5_matview": "+ materialized views",
    "v6_projection": "alt: projections",
}


def _fmt_rows(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}k"
    return str(n)


def _esc(text: str) -> str:
    return (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def build_svg(results: dict, queries: list[str]) -> str:
    plot_w = WIDTH - LEFT - RIGHT
    parts: list[str] = []
    y = 16

    for query_key in queries:
        rows = []
        for stage_key, stage in results["stages"].items():
            measured = stage["queries"].get(query_key)
            if measured:
                rows.append((stage_key, stage["label"], measured))
        if not rows:
            continue

        baseline = rows[0][2]["p95_ms"]
        worst = max(r[2]["p95_ms"] for r in rows)

        parts.append(
            f'<text x="12" y="{y}" font-size="13" font-weight="700" '
            f'fill="{COLOR_TEXT}" font-family="system-ui,sans-serif">'
            f'{_esc(QUERY_TITLES.get(query_key, query_key))}</text>'
        )
        y += 20

        for stage_key, label, measured in rows:
            p95 = measured["p95_ms"]
            # Linear scale: the point is that the baseline dwarfs everything.
            # A minimum width keeps the fast bars visible rather than invisible.
            bar_w = max(3, int(plot_w * p95 / worst))
            if stage_key == "v0_naive":
                color = COLOR_SLOW
            elif p95 == min(r[2]["p95_ms"] for r in rows):
                color = COLOR_FAST
            else:
                color = COLOR_MID
            speedup = baseline / p95 if p95 else 0

            short = SHORT_LABELS.get(stage_key, label.split("(")[0].strip())
            parts.append(
                f'<text x="{LEFT - 10}" y="{y + BAR_H - 3}" text-anchor="end" '
                f'font-size="11.5" fill="{COLOR_LABEL}" '
                f'font-family="ui-monospace,monospace">{_esc(short)}</text>'
            )
            parts.append(
                f'<rect x="{LEFT}" y="{y}" width="{bar_w}" height="{BAR_H}" '
                f'rx="2.5" fill="{color}"/>'
            )
            suffix = "" if stage_key == "v0_naive" else f" ({speedup:.0f}×)"
            parts.append(
                f'<text x="{LEFT + bar_w + 8}" y="{y + BAR_H - 3}" '
                f'font-size="11.5" fill="{COLOR_TEXT}" '
                f'font-family="ui-monospace,monospace">'
                f'{p95:.1f} ms{suffix}  ← {_fmt_rows(measured["rows_read"])}'
                f' rows</text>'
            )
            y += ROW_H
        y += GROUP_GAP

    footer = (
        f'{results["rows"]:,} events · p95 over {results["repeat"]} '
        f'interleaved rounds · every stage verified to return identical '
        f'results · bar length is linear in latency'
    )
    parts.append(
        f'<text x="12" y="{y - GROUP_GAP + 16}" font-size="10.5" '
        f'fill="{COLOR_LABEL}" font-family="system-ui,sans-serif">'
        f'{_esc(footer)}</text>'
    )
    height = y - GROUP_GAP + 30

    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" '
        f'height="{height}" viewBox="0 0 {WIDTH} {height}" '
        f'role="img" aria-label="ClickHouse tuning results">\n'
        + "\n".join(parts)
        + "\n</svg>\n"
    )


def write_chart(results: dict, path: str | None = None) -> str:
    """Render ``results`` and return the path written."""
    path = path or os.path.join(RESULTS_DIR, "tuning.svg")
    queries = [q for q in ("q1_tenant_dashboard", "q3_platform_wide")
               if any(q in s["queries"] for s in results["stages"].values())]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as handle:
        handle.write(build_svg(results, queries))
    return path


def main() -> None:
    source = os.path.join(RESULTS_DIR, "latest.json")
    if not os.path.exists(source):
        raise SystemExit(f"No results at {source}; run bench_clickhouse.py first")
    with open(source) as handle:
        results = json.load(handle)
    print(f"Wrote {write_chart(results)}")


if __name__ == "__main__":
    main()
