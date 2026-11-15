"""Render SVG and PNG figures from trial summaries without weighting by request count."""

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib.ticker import FuncFormatter, LogLocator
import numpy as np

from .data import BACKENDS, FAILURES, POLICIES, RECOVERY, SCENARIOS, SUDDEN
from .statistics import BIN_SECONDS, EVENT_WINDOW

SCENARIO_LABELS = dict(zip(SCENARIOS, ("Stable heterogeneous", "Sudden degradation", "Degradation and recovery")))
POLICY_LABELS = dict(zip(POLICIES, ("First Eligible", "Round Robin", "Least Requests", "P2C", "P2C + Peak EWMA",
                                 "UCB1", "Sliding-Window UCB", "Discounted UCB")))
COLORS = ("#454b56", "#956533", "#087875", "#73539c", "#2668aa", "#bb364d", "#b07500", "#287f44")
LINESTYLES = ("--", ":", "-", "-.", "-", "-", "--", "-.")
MARKERS = ("o", "s", "^", "D", "v", "P", "X", "h")
BACKEND_COLORS = ("#0072b2", "#e69f00", "#009e73", "#cc79a7", "#56b4e9")
FAILURE_COLORS = ("#585c70", "#d55e00", "#0072b2", "#cc79a7", "#009e73")
FAILURE_LABELS = ("Transport", "Backend 503", "Proxy 503", "Unclassified 503", "Other HTTP")
POSITIONS = np.arange(len(POLICIES))[::-1]
DPI = 250

FIGURES = {
    "latency": "Successful k6 latency by percentile. Marks are medians of trial-level percentiles and whiskers span the observed repetitions. The axis is logarithmic.",
    "throughput": "Total and HTTP 200 requests/s per scheduled workload second. Whiskers span the observed repetitions. The dashed line is the scheduled mean rate.",
    "reliability": "Mean trial percentages for success and the failure categories. Every trial carries equal weight. Success and failures use the full client outcome counts.",
    "routing": "HTTP 200 backend response shares among identified successes across all three scenarios. The fourth panel shows measured assignments across all outcomes in the traced campaign.",
    "degradation": "Five-second bins relative to degradation. Medians group trial-level values. Client success excludes unassigned dispatches and routing panels identify their source.",
    "recovery": "Five-second bins relative to recovery. Median successful latency and success use estimated client dispatch. Backend A assignment share uses proxy selection time.",
    "queue-overload": "Traced phases only. Queue cells show median observed peaks and gray marks unavailable measurements. Load is sampled before selection. Rejections use a shared symmetric-log scale that includes zero.",
    "matched-variability": "Three seed-matched differences against Round Robin per policy and scenario. Bars span the observed differences and the diamond marks the median. Zero means no difference.",
}


def frame(title, columns=3, rows=1, width=14, height=4.5, **kwargs):
    fig, axes = plt.subplots(rows, columns, figsize=(width, height), squeeze=False, layout="constrained", **kwargs)
    fig.suptitle(title, fontsize=16, weight="bold")
    return fig, axes


def policy_axis(ax):
    ax.set_yticks(POSITIONS, [POLICY_LABELS[p] for p in POLICIES])
    ax.set_ylim(-.6, len(POLICIES) - .4)
    ax.grid(axis="x", alpha=.22)
    ax.set_axisbelow(True)


def lookup(aggregates):
    return {(row["scenario"], row["policy"], row["scope"], row["offset_seconds"]): row["metrics"]
            for row in aggregates}


def values(index, scenario, metric, scope="whole", field="median"):
    return [index.get((scenario, p, scope, None), {}).get(metric, {}).get(field, np.nan) for p in POLICIES]


def intervals(ax, index, scenario, metric, y, color, marker="o"):
    central = np.array(values(index, scenario, metric))
    extent = [central - values(index, scenario, metric, field="min"),
              np.array(values(index, scenario, metric, field="max")) - central]
    ax.errorbar(central, y, xerr=extent, fmt=marker, color=color, markersize=4, capsize=2, linewidth=1)


def latency(index):
    fig, axes = frame("Successful-request latency by policy", sharex=True)
    percentiles = ("p50", "p95", "p99")
    shades = ("#0072b2", "#007b65", "#c65313")
    markers = ("o", "s", "^")
    for ax, scenario in zip(axes[0], SCENARIOS):
        policy_axis(ax)
        ax.set_title(SCENARIO_LABELS[scenario])
        ax.set_xscale("log")
        ax.set_xlabel("HTTP 200 latency (ms, log scale)")
        for offset, percentile, shade, mark in zip((.22, 0, -.22), percentiles, shades, markers):
            intervals(ax, index, scenario, f"{percentile}_ms", POSITIONS + offset, shade, mark)
    low = min(v for s in SCENARIOS for v in values(index, s, "p50_ms", field="min"))
    high = max(v for s in SCENARIOS for v in values(index, s, "p99_ms", field="max"))
    axes[0, 0].set_xlim(low * .8, high * 1.25)
    axes[0, 0].xaxis.set_major_locator(LogLocator(subs=(1, 2, 5)))
    axes[0, 0].xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}"))
    fig.legend(handles=[Line2D([], [], marker=m, color=c, label=p, linestyle="")
                        for p, c, m in zip(percentiles, shades, markers)],
               loc="outside lower center", ncol=3, title="Percentile", frameon=False)
    return fig


def throughput(index, offered):
    fig, axes = frame("Offered and successful throughput", sharex=True)
    for ax, scenario in zip(axes[0], SCENARIOS):
        policy_axis(ax)
        ax.set_title(SCENARIO_LABELS[scenario])
        ax.set_xlim(left=0)
        ax.set_xlabel("Requests per scheduled second")
        line = ax.axvline(offered[scenario], color="#737983", linestyle="--", linewidth=1, label="Scheduled")
        intervals(ax, index, scenario, "total_rps", POSITIONS + .14, "#485364", "s")
        intervals(ax, index, scenario, "successful_rps", POSITIONS - .14, "#087875")
    axes[0, -1].legend(handles=[line, Line2D([], [], marker="s", color="#485364", linestyle="", label="Total"),
                                Line2D([], [], marker="o", color="#087875", linestyle="", label="HTTP 200")],
                       loc="lower left")
    return fig


def stacked(ax, series, colors):
    left = np.zeros(len(POLICIES))
    for segment, color in zip(series, colors):
        segment = np.asarray(segment)
        ax.barh(POSITIONS, segment, left=left, height=.62, color=color, edgecolor="white", linewidth=.45)
        left += np.nan_to_num(segment)


def reliability(index):
    fig, axes = frame("Client success and failure composition", rows=2, height=8)
    max_failure = max(100 - min(values(index, s, "success_pct", field="mean")) for s in SCENARIOS)
    failure_limit = min(100, max(5, max_failure * 1.1))
    for scenario, top, bottom in zip(SCENARIOS, axes[0], axes[1]):
        for ax in (top, bottom):
            policy_axis(ax)
        top.set_title(SCENARIO_LABELS[scenario])
        top.set_xlim(0, 100)
        top.set_xlabel("HTTP 200 responses (% of all observations)")
        top.barh(POSITIONS, values(index, scenario, "success_pct", field="mean"), height=.62, color="#087875")
        bottom.set_xlim(0, failure_limit)
        bottom.set_xlabel("Failures (% of all observations)")
        stacked(bottom, [values(index, scenario, f"{kind}_pct", field="mean") for kind in FAILURES], FAILURE_COLORS)
    axes[1, 0].legend(handles=[Patch(color=color, label=label) for color, label in zip(FAILURE_COLORS, FAILURE_LABELS)],
                      loc="lower right", fontsize=8)
    return fig


def routing(index):
    fig, axes = frame("Backend traffic distribution", columns=2, rows=2, width=11.5, height=8)
    for ax, scenario, basis in zip(axes.flat, (*SCENARIOS, RECOVERY),
                                   ("successful_response", "successful_response", "successful_response", "assignment")):
        policy_axis(ax)
        ax.set_title(SCENARIO_LABELS[scenario])
        ax.set_xlim(0, 100)
        label = "Share of identified HTTP 200 responses (%)" if basis == "successful_response" else "Share of all proxy selections (%)"
        ax.set_xlabel(label)
        stacked(ax, [values(index, scenario, f"{b}_{basis}_pct", field="mean") for b in BACKENDS], BACKEND_COLORS)
    fig.legend(handles=[Patch(color=color, label=b[-1].upper()) for b, color in zip(BACKENDS, BACKEND_COLORS)],
               loc="outside lower center", ncol=5, frameon=False, title="Backend")
    return fig


def timeline(index, event):
    scenarios = (SUDDEN, RECOVERY) if event == "degradation" else (RECOVERY,)
    fig, axes = frame(f"Response around backend {event}", columns=len(scenarios), rows=3,
                      width=13 if len(scenarios) == 2 else 10, height=9.4, sharex=True, sharey="row")
    offsets = tuple(range(*EVENT_WINDOW, BIN_SECONDS))
    handles = []
    for column, scenario in enumerate(scenarios):
        routing_metric = "backend-a_assignment_pct" if scenario == RECOVERY else "backend-a_successful_response_pct"
        metrics = ("p95_ms", "success_pct", routing_metric)
        labels = ("HTTP 200 p95 (ms, log scale)", "HTTP 200 (%)",
                  "A share of proxy selections (%)" if scenario == RECOVERY else "A share of identified HTTP 200 (%)")
        axes[0, column].set_title(SCENARIO_LABELS[scenario])
        for i, (metric, label) in enumerate(zip(metrics, labels)):
            ax = axes[i, column]
            ax.set_ylabel(label)
            ax.axvline(0, color="#737983", linewidth=1.1)
            ax.grid(alpha=.22)
            if i == 0:
                ax.set_yscale("log")
            else:
                ax.set_ylim(0, 102)
            for p, color, style, marker in zip(POLICIES, COLORS, LINESTYLES, MARKERS):
                points = [index.get((scenario, p, f"event_{event}", offset), {}).get(metric, {}).get("median", np.nan)
                          for offset in offsets]
                line, = ax.plot(np.asarray(offsets) + BIN_SECONDS / 2, points, color=color, linestyle=style,
                                label=POLICY_LABELS[p], marker=marker, markersize=3)
                if column == 0 and i == 0:
                    handles.append(line)
        axes[-1, column].set_xlabel(f"Seconds relative to {event} (bin midpoint)")
    fig.legend(handles=handles, loc="outside lower center", ncol=4, fontsize=8)
    return fig


def queue_overload(index):
    fig, axes = frame("Queueing under degradation and recovery", rows=3, height=10.4, sharex="row")
    for (phase, title), top, middle, bottom in zip(
        (("normal", "Normal"), ("degraded", "Degradation"), ("recovery", "Recovery")), *axes,
    ):
        for ax in (top, middle, bottom):
            policy_axis(ax)
        top.set_title(title)
        depths = np.array([values(index, RECOVERY, f"{b}_queue_peak", phase) for b in BACKENDS]).T
        cmap = matplotlib.colormaps["viridis"].with_extremes(bad="#e5e7eb")
        heatmap = top.imshow(depths, extent=(-.5, 4.5, -.5, 7.5), aspect="auto", cmap=cmap, vmin=0, vmax=32)
        top.set_xticks(range(5), [b[-1].upper() for b in BACKENDS])
        top.set_xlabel("Backend")
        top.grid(False)
        middle.barh(POSITIONS, values(index, RECOVERY, "mean_selection_in_flight", phase), height=.62, color="#087875")
        middle.set_xlim(left=0)
        middle.set_xlabel("Mean outstanding requests at selection")
        bottom.scatter(values(index, RECOVERY, "backend_queue_full_attempt_pct", phase), POSITIONS,
                       color="#087875", s=20, zorder=3, clip_on=False)
        bottom.set_xscale("symlog", linthresh=.1)
        bottom.set_xlim(0, 100)
        bottom.set_xticks([0, .1, 1, 10, 100], ["0", "0.1", "1", "10", "100"])
        bottom.set_xlabel("Queue-full rejections (% of attempts, symlog)")
    fig.colorbar(heatmap, ax=axes[0, :].tolist(), label="Observed peak queue (requests)", shrink=.84)
    return fig


def matched_variability(effects):
    table = {(r["scenario"], r["policy"], r["metric"]): r for r in effects if r["scope"] == "whole"}
    fig, axes = frame("Seed-matched differences from Round Robin", rows=2, height=8, sharex="row")
    for scenario, top, bottom in zip(SCENARIOS, axes[0], axes[1]):
        top.set_title(SCENARIO_LABELS[scenario])
        for ax, metric, label in ((top, "p99_ms", "HTTP 200 p99 difference (ms)"),
                                  (bottom, "success_pct", "HTTP 200 rate difference (percentage points)")):
            policy_axis(ax)
            ax.set_yticks([y for p, y in zip(POLICIES, POSITIONS) if p != "round-robin"],
                          [POLICY_LABELS[p] for p in POLICIES if p != "round-robin"])
            ax.axvline(0, color="#737983", linewidth=1)
            ax.set_xlabel(label)
            for policy, position, color in zip(POLICIES, POSITIONS, COLORS):
                row = table.get((scenario, policy, metric))
                if row is None:
                    continue
                ax.hlines(position, row["min"], row["max"], color=color, linewidth=1)
                ax.scatter([p["delta"] for p in row["pairs"]],
                           position + np.linspace(-.12, .12, len(row["pairs"])), color=color, s=15, alpha=.7)
                ax.scatter(row["median"], position, marker="D", color=color, s=24, zorder=3)
    return fig


def save_figure(fig, base):
    try:
        fig.savefig(base.with_suffix(".svg"), metadata={"Date": None})
        fig.savefig(base.with_suffix(".png"), dpi=DPI)
    finally:
        plt.close(fig)


def render_all(aggregates, comparisons, offered, directory):
    index = lookup(aggregates)
    with plt.rc_context({"font.family": "DejaVu Sans", "font.size": 9, "axes.spines.top": False,
                         "axes.spines.right": False, "svg.fonttype": "none", "svg.hashsalt": "sbr-analysis"}):
        for name, render in (
            ("latency", lambda: latency(index)), ("throughput", lambda: throughput(index, offered)),
            ("reliability", lambda: reliability(index)), ("routing", lambda: routing(index)),
            ("degradation", lambda: timeline(index, "degradation")), ("recovery", lambda: timeline(index, "recovery")),
            ("queue-overload", lambda: queue_overload(index)), ("matched-variability", lambda: matched_variability(comparisons)),
        ):
            save_figure(render(), directory / name)
