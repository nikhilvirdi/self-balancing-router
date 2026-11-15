"""Write the statistics, provenance, and readable comparison report."""

from collections import Counter
import csv
import json
import os

from .data import PILOT_REPORT, POLICIES, RECOVERY, RECOVERY_REPORT, SCENARIOS, STABLE, SUDDEN
from .plots import FIGURES, POLICY_LABELS, SCENARIO_LABELS, lookup


def write_statistics(path, trials, aggregates):
    fields = ("level", "scenario", "policy", "scope", "offset_seconds", "window_seconds", "trial", "block", "seed",
              "metric", "value", "n", "median", "min", "max", "mean", "sd")
    with path.open("x", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for level, rows in (("trial", trials), ("aggregate", aggregates)):
            for row in rows:
                info = {field: row[field] for field in fields if field in row}
                for metric, value in sorted(row["metrics"].items()):
                    measure = {"value": value} if level == "trial" else value
                    writer.writerow({"level": level, "metric": metric, **info, **measure})


def write_json(path, content):
    with path.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(content, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")


def archive_link(filename, root, output):
    relative = os.path.relpath(root / filename, output).replace("\\", "/")
    return f"[{filename.rsplit('/', 1)[-1]}]({relative})"


def typical(table, scenario, policy, metric, scope="whole"):
    return table[scenario, policy, scope, None][metric]["median"]


def med_range(stats, digits=2):
    return f"{stats['median']:.{digits}f} [{stats['min']:.{digits}f}–{stats['max']:.{digits}f}]"


def write_summary(output, root, trials, aggregates, effects, metadata):
    table = lookup(aggregates)
    registry = {row["path"]: row for row in metadata["trials"]}
    lines = [
        "# Matched load-balancing analysis", "",
        f"{len(registry)} comparison trials, eight policies, and three matched seed blocks per scenario. "
        "Each scenario is analyzed independently.", "",
        "[Statistics](statistics.csv) · [Matched comparisons](comparisons.json) · [Provenance](analysis-metadata.json)",
        "", "## Cohorts and methods", "",
        f"The pilot and recovery manifests define the comparison cohorts. Validation controls, smoke tests and trials outside "
        f"the manifest selection stay in the archive. The successful revalidation replaces the initial audit decision for the same "
        f"Sliding-Window UCB trial. Archived references include the {archive_link(PILOT_REPORT, root, output)} and "
        f"{archive_link(RECOVERY_REPORT, root, output)} reports.", "",
        "- The pilot cohorts ran for 90 seconds with a 40/80/40 requests/s schedule. The traced recovery cohort ran at 80 requests/s "
        "over 160 seconds, used direct Compose ingress, and included a reset. Backend A's degraded jitter also differs between campaigns. "
        "Scenario results use separate rows and effect estimates.",
        "- Compute linearly interpolated percentiles within each trial using successful HTTP 200 durations. Throughput divides counts by the "
        "scheduled trial or attribution-window duration. Whole-trial outcomes include requests that complete after the scheduled end. "
        "Archived pilot throughput uses elapsed completion span, which can differ when completions extend beyond the workload.",
        "- Repetitions are the units of aggregation. Tables show medians and observed ranges, while the CSV also includes means, sample standard "
        "deviations, individual trials and observed sample counts. Success and throughput count failures separately from successful latency.",
        "- Matched effects subtract the Round Robin result for the same scenario, phase, block and seed. Report median absolute differences "
        "in metric units, including percentage points for rates. Three independent continuous pairs give only 75% coverage for their full "
        "range as a distribution-free median interval. The structured 95% CI fields are empty at this sample size. Ranges are descriptive.",
        "- Client event windows use completion minus recorded k6 request duration as an approximate dispatch time. Status-0 observations "
        "with zero recorded duration retain their whole-trial failures and have no timed dispatch attribution. Figures use full five-second "
        "bins centered relative to the event. Counts of requests spanning changes are retained separately.",
        "- For the recovery campaign, event time uses the midpoint between the recorded admin trigger and acknowledgement. For the older pilot, "
        "it uses the recorded relative trigger plus half the admin request latency. The latter also inherits start-synchronization uncertainty. "
        "Small timing changes can move observations across a boundary.",
        "- Backend response shares in the pilot use identified HTTP 200 observations. Error responses generally lack backend identity. "
        "The traced campaign separately reports actual proxy assignments, in-flight samples at selection, and backend event queue peaks and rejections. "
        "Proxy forwarding duration is separately labeled and carries a different clock and request boundary from k6.",
        "- Telemetry sampled every request in the recovery campaign. Before/after counters and trace records were audited together. The archived "
        "Least Requests instrumentation control covers one seed and policy. Its phase p95 shift was at most "
        f"{metadata['instrumentation_control_max_p95_delta_ms']:.3f} ms. Queue peaks reflect observed events. Missing series remain unavailable.",
        "", "## Whole-trial comparisons", "",
        "Each cell shows the median, with observed repetition bounds in brackets where shown. Read success rate alongside successful latency.",
    ]
    for scenario in SCENARIOS:
        lines += ["", f"### {SCENARIO_LABELS[scenario]}", "",
                  "| Policy | Trials | p50 / p95 / p99 HTTP 200 (ms) | Total / successful requests/s | HTTP 200 (%) | Backend 503 (%) |",
                  "|---|---:|---|---|---|---:|"]
        for policy in POLICIES:
            m = table[scenario, policy, "whole", None]
            lat = f"{m['p50_ms']['median']:.1f} / {m['p95_ms']['median']:.1f} / {med_range(m['p99_ms'], 1)}"
            lines.append(f"| {POLICY_LABELS[policy]} | {m['requests']['n']} | {lat} | {m['total_rps']['median']:.2f} / "
                         f"{med_range(m['successful_rps'])} | {med_range(m['success_pct'])} | {m['backend_503_pct']['median']:.2f} |")
    lines += ["", "## Event phases", "",
              "For timed phases, rates use the observed dispatch subset and the duration of each phase. "
              "The missing pilot timeout dispatches can affect both phase reliability and offered rates. Full-trial reliability includes them.",
              "", "| Scenario and phase | Policy | Successful requests/s | HTTP 200 (%) | HTTP 200 p99 (ms) |",
              "|---|---|---|---|---|"]
    for scenario, scopes in ((SUDDEN, ("normal", "degraded")), (RECOVERY, ("normal", "degraded", "recovery"))):
        for scope in scopes:
            for policy in POLICIES:
                m = table[scenario, policy, scope, None]
                lines.append(f"| {SCENARIO_LABELS[scenario]} · {scope} | {POLICY_LABELS[policy]} | "
                             f"{med_range(m['successful_rps'])} | {med_range(m['success_pct'])} | {med_range(m['p99_ms'], 1)} |")
    lines += ["", "## Evidence from these trials", ""]
    stable_tail = typical(table, STABLE, "sliding-window-ucb", "p99_ms")
    stable_ref = typical(table, STABLE, "round-robin", "p99_ms")
    stable_success = typical(table, STABLE, "sliding-window-ucb", "success_pct")
    lines.append(f"- In the stable workload, Sliding-Window UCB had median successful p99 of {stable_tail:.1f} ms versus "
                 f"{stable_ref:.1f} ms for Round Robin, with {stable_success:.2f}% median client success for Sliding-Window UCB.")
    paired = next(row for row in effects if row["scenario"] == SUDDEN and row["policy"] == "least-requests"
                  and row["scope"] == "whole" and row["metric"] == "p99_ms")
    lines.append(f"- In the sudden degradation pilot, the seed-paired Least Requests minus Round Robin p99 difference was "
                 f"{paired['median']:+.1f} ms, with observed differences from {paired['min']:+.1f} to {paired['max']:+.1f} ms. "
                 "Whole-trial transport failures remain in the reliability table.")
    lr = typical(table, RECOVERY, "least-requests", "successful_rps", "degraded")
    first = typical(table, RECOVERY, "first-eligible", "successful_rps", "degraded")
    tail = table[RECOVERY, "sliding-window-ucb", "recovery", None]["p99_ms"]
    lines.append(f"- In the traced degradation phase, Least Requests sustained a median {lr:.2f} successful requests/s, "
                 f"and First Eligible sustained {first:.2f}. Sliding-Window UCB recovery p99 ranged "
                 f"from {tail['min'] / 1000:.2f} to {tail['max'] / 1000:.2f} seconds across the three trials.")
    lines += ["", "## Data-quality audit", "",
              f"The loader reconciled raw k6 observations, the request CSV, k6 and derived summaries, and manifest counts "
              f"for all {len(registry)} trials. The archive cross-check reproduced "
              f"{metadata['reference_counts']['pilot_policy_scenario_percentile_rows']} pilot policy/scenario percentile rows "
              f"and {metadata['reference_counts']['recovery_trial_phase_rows']} traced trial-phase rows. "
              "Older queue tables encode no queue observation as zero. This analysis preserves it as an empty value.",
              "", "| Scenario | Requests | Status 0 with no dispatch estimate | Timeout tags by k6 phase | Event-spanning client and proxy observations |",
              "|---|---:|---:|---|---|"]
    for scenario in SCENARIOS:
        whole = [r for r in trials if r["scenario"] == scenario and r["scope"] == "whole"]
        timeouts = Counter()
        for row in whole:
            timeouts.update(registry[row["trial"]]["quality"]["transport_by_k6_phase"])
        statuses = ", ".join(f"{k} {v:,}" for k, v in sorted(timeouts.items())) or "None observed"
        total = sum(r["metrics"]["requests"] for r in whole)
        unassigned = sum(r["metrics"]["unassigned_dispatches"] for r in whole)
        client = sum(r["metrics"]["boundary_spans"] for r in whole)
        proxy = sum(r["metrics"].get("proxy_boundary_spans", 0) for r in whole)
        lines.append(f"| {SCENARIO_LABELS[scenario]} | {total:,} | {unassigned:,} | {statuses} | "
                     f"{client:,} / {format(proxy, ',') if scenario == RECOVERY else 'unavailable'} |")
    lines += ["", "The k6 phase tags show some timeouts outside warmup. The records establish when k6 labeled the request "
              "but leave the cause and exact dispatch time of zero-duration failures unresolved. The historical boundary counts and "
              "completion-span rates are retained per trial in the audit metadata. Traced event order inversions within the documented "
              "clock tolerance are recorded separately from missing events.", "",
              "## Figures", "",
              "| Figure | Files | Reading guide |", "|---|---|---|"]
    for name, caption in FIGURES.items():
        lines.append(f"| {name.replace('-', ' ').capitalize()} | [SVG](figures/{name}.svg) · [PNG](figures/{name}.png) | {caption} |")
    lines += ["", "## Scope of inference", "",
              "These are exploratory comparisons on three seed blocks per workload and one recorded environment. "
              "The paired effects quantify observed changes and preserve repeat-to-repeat variation. "
              "They leave uncertainty about behavior under other rates, backend characteristics and instrumentation settings. "
              "The observed delayed completions and queue peaks support inspection of feedback timing, but cannot identify "
              "a causal chain from a bandit update to a burst.", ""]
    with (output / "summary.md").open("x", encoding="utf-8", newline="\n") as stream:
        stream.write("\n".join(lines))


def append_index(index, output, date, trial_count):
    link = output.relative_to(index.parent).as_posix()
    with index.open("a+", encoding="utf-8", newline="\n") as stream:
        if stream.tell() == 0:
            stream.write("# Offline analyses\n\n")
        stream.write(f"- {date[:10]} UTC · [Matched campaign analysis]({link}/summary.md) · "
                     f"{trial_count} trials across stable, sudden degradation, and recovery workloads\n")
