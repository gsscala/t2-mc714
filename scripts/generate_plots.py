"""Generates publication-ready figures for the IEEE report from benchmark data."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

DATA_DIR = Path("data")
FIG_DIR = Path("figures")
FIG_DIR.mkdir(parents=True, exist_ok=True)

SCEN_LABELS = {
    "normal": "Normal",
    "contended": "Contended",
    "degraded": "Degraded (10 ms)",
    "calm": "Calm",
    "jittery": "Jittery (8 ms)",
}
SCEN_COLORS = {
    "normal": "#1f77b4",
    "contended": "#9467bd",
    "degraded": "#d62728",
    "calm": "#2ca02c",
    "jittery": "#ff7f0e",
}

# Consistent matplotlib styling for the IEEE two-column format
plt.rcParams.update({
    "font.size": 10,
    "font.family": "serif",
    "axes.labelsize": 10,
    "axes.titlesize": 11,
    "xtick.labelsize": 9,
    "ytick.labelsize": 9,
    "legend.fontsize": 8.5,
    "figure.titlesize": 12,
    "lines.linewidth": 1.8,
    "lines.markersize": 6,
    "grid.alpha": 0.35,
    "grid.linestyle": "--",
})


def plot_exp1():
    csv_file = DATA_DIR / "exp1_ricart_agrawala.csv"
    if not csv_file.exists():
        print(f"Skipping exp1: {csv_file} not found")
        return

    df = pd.read_csv(csv_file)

    # 1. CS entry latency (p50 with p95 band) per scenario
    fig, ax = plt.subplots(figsize=(4.8, 3.2), dpi=300)
    for scen in ["normal", "contended", "degraded"]:
        sub = df[df["scenario"] == scen]
        if sub.empty:
            continue
        grouped = sub.groupby("cluster_size")["latency_ms"]
        sizes = grouped.mean().index
        p50 = grouped.quantile(0.50)
        p95 = grouped.quantile(0.95)

        color = SCEN_COLORS[scen]
        ax.plot(sizes, p50, color=color, marker="o", label=f"{SCEN_LABELS[scen]} (p50)")
        ax.plot(sizes, p95, color=color, linestyle=":", alpha=0.7, label=f"{SCEN_LABELS[scen]} (p95)")
        ax.fill_between(sizes, p50, p95, color=color, alpha=0.12)

    ax.set_xlabel("Cluster Size (N Nodes)")
    ax.set_ylabel("CS Entry Latency (ms)")
    ax.set_title("Ricart-Agrawala Acquisition Latency")
    ax.set_xticks(sorted(df["cluster_size"].unique()))
    ax.grid(True)
    ax.legend(loc="upper left", ncol=2)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "exp1_ra_latency.pdf")
    fig.savefig(FIG_DIR / "exp1_ra_latency.png")
    plt.close(fig)

    # 2. Message complexity: empirical vs theoretical 2(N-1), solo entries
    fig, ax = plt.subplots(figsize=(4.8, 3.2), dpi=300)
    sub = df[df["scenario"] == "normal"]
    grouped = sub.groupby("cluster_size")["total_messages"]
    sizes = np.array(grouped.mean().index)
    means = grouped.mean().values
    stds = grouped.std().fillna(0).values

    theoretical = 2 * (sizes - 1)
    # Theory as a thick dashed reference line; empirical points on top. The
    # measurements coincide exactly with 2(N-1), so a solid line would hide
    # the reference — markers let the theory show through between points.
    ax.plot(sizes, theoretical, "r--", linewidth=3.0, label="Theoretical 2(N-1)")
    ax.errorbar(sizes, means, yerr=stds, fmt="o", color="#1f77b4", markersize=8,
                capsize=4, markeredgecolor="black", markeredgewidth=0.6,
                label="Empirical Messages")

    ax.set_xlabel("Cluster Size (N Nodes)")
    ax.set_ylabel("Total Messages per CS Entry")
    ax.set_title("Ricart-Agrawala Message Complexity")
    ax.set_xticks(sorted(df["cluster_size"].unique()))
    ax.grid(True)
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(FIG_DIR / "exp1_ra_messages.pdf")
    fig.savefig(FIG_DIR / "exp1_ra_messages.png")
    plt.close(fig)
    print("Generated Exp 1 plots.")


def plot_exp2():
    csv_file = DATA_DIR / "exp2_two_phase_commit.csv"
    if not csv_file.exists():
        print(f"Skipping exp2: {csv_file} not found")
        return

    df = pd.read_csv(csv_file)

    # 1. 2PC commit latency vs injected channel delay (p50/p95)
    sub_normal = df[df["scenario"] == "normal_commit"]
    grouped = sub_normal.groupby("delay_ms")["commit_latency_ms"]
    delays = np.array(grouped.mean().index)
    p50 = grouped.quantile(0.50).values
    p95 = grouped.quantile(0.95).values

    fig, ax = plt.subplots(figsize=(4.8, 3.2), dpi=300)
    ax.plot(delays, p50, "go-", label="p50 Latency")
    ax.plot(delays, p95, "b^--", label="p95 Latency")
    ax.fill_between(delays, p50, p95, color="gray", alpha=0.15)

    ax.set_xlabel("Injected Channel Delay (ms)")
    ax.set_ylabel("Commit Latency (ms)")
    ax.set_title("2PC Transaction Commit Latency")
    ax.set_xticks(delays)
    ax.grid(True)
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(FIG_DIR / "exp2_2pc_latency.pdf")
    fig.savefig(FIG_DIR / "exp2_2pc_latency.png")
    plt.close(fig)

    # 2. Recovery duration after a real coordinator process crash
    sub_crash = df[df["scenario"].isin(["crash_before_decision", "crash_after_decision"])]
    fig, ax = plt.subplots(figsize=(4.8, 3.2), dpi=300)
    labels = {"crash_before_decision": "Crash before decision\n(Presumed Abort)",
              "crash_after_decision": "Crash after decision\n(re-decision + re-propagation)"}
    width = 0.35
    x = np.arange(len(delays))
    for offset, (scen, color) in zip([-width / 2, width / 2],
                                     [("crash_before_decision", "#ff7f0e"),
                                      ("crash_after_decision", "#8c564b")]):
        sub = sub_crash[sub_crash["scenario"] == scen]
        grouped = sub.groupby("delay_ms")["recovery_duration_ms"]
        means = grouped.mean().reindex(delays)
        stds = grouped.std().fillna(0).reindex(delays)
        ax.bar(x + offset, means.values, width, yerr=stds.values, capsize=4,
               color=color, edgecolor="black", alpha=0.85, label=labels[scen])

    ax.set_xticks(x)
    ax.set_xticklabels([f"{int(d)} ms" for d in delays])
    ax.set_xlabel("Injected Channel Delay (ms)")
    ax.set_ylabel("Recovery Duration (ms)")
    ax.set_title("2PC Coordinator Crash Recovery (real process crash)")
    ax.grid(True, axis="y")
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(FIG_DIR / "exp2_2pc_recovery.pdf")
    fig.savefig(FIG_DIR / "exp2_2pc_recovery.png")
    plt.close(fig)
    print("Generated Exp 2 plots.")


def plot_exp3():
    csv_file = DATA_DIR / "exp3_chandy_lamport.csv"
    if not csv_file.exists():
        print(f"Skipping exp3: {csv_file} not found")
        return

    df = pd.read_csv(csv_file)

    # 1. Snapshot convergence time vs transfer rate
    fig, ax = plt.subplots(figsize=(4.8, 3.2), dpi=300)
    for scen in ["calm", "jittery"]:
        sub = df[df["scenario"] == scen]
        grouped = sub.groupby("transfer_rate")["snapshot_duration_ms"]
        rates = grouped.mean().index
        p50 = grouped.quantile(0.50).values
        p95 = grouped.quantile(0.95).values
        color = SCEN_COLORS[scen]

        ax.plot(rates, p50, color=color, marker="o", label=f"{SCEN_LABELS[scen]} (p50)")
        ax.plot(rates, p95, color=color, linestyle="--", alpha=0.7, label=f"{SCEN_LABELS[scen]} (p95)")

    ax.set_xlabel("Concurrent Transfer Rate (tx/s)")
    ax.set_ylabel("Snapshot Duration (ms)")
    ax.set_title("Chandy-Lamport Convergence Time")
    ax.set_xticks(sorted(df["transfer_rate"].unique()))
    ax.grid(True)
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(FIG_DIR / "exp3_snapshot_duration.pdf")
    fig.savefig(FIG_DIR / "exp3_snapshot_duration.png")
    plt.close(fig)

    # 2. Invariant conservation across all trials
    fig, ax = plt.subplots(figsize=(4.8, 3.2), dpi=300)
    totals = df["computed_total"].values
    expected = float(np.median(totals))
    consistent = int(df["is_consistent"].sum())
    ax.hist(totals, bins=10, color="#17becf", edgecolor="black", alpha=0.8)
    ax.axvline(expected, color="red", linestyle="--", linewidth=2, label=f"Expected Total (${expected:,.0f})")
    ax.set_xlabel("Audited System Balance ($)")
    ax.set_ylabel("Count of Snapshots")
    ax.set_title(f"Balance Conservation Invariant ({consistent}/{len(df)} Consistent)")
    ax.grid(True)
    ax.legend()
    fig.tight_layout()
    fig.savefig(FIG_DIR / "exp3_snapshot_conservation.pdf")
    fig.savefig(FIG_DIR / "exp3_snapshot_conservation.png")
    plt.close(fig)
    print("Generated Exp 3 plots.")


if __name__ == "__main__":
    plot_exp1()
    plot_exp2()
    plot_exp3()
    print("All plots generated successfully in figures/")
