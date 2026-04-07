"""
visualize.py
------------
Visualise extracted scenario samples to validate the extraction process.

Generates two types of plots for each scenario type:
  1. **Trajectory plot** – ego + surrounding vehicle paths (local_x / local_y)
     over the 5-second window, colour-coded by vehicle role.
  2. **Summary bar chart** – count of each scenario type across the full run.

Usage
-----
    python visualize.py --csv output/scenario_samples.csv --output_dir output/plots

Output
------
  output/plots/summary_counts.png   – scenario type bar chart
  output/plots/car_following_<id>.png
  output/plots/lane_change_<id>.png
  output/plots/overtaking_<id>.png
  (one sample plot per scenario type, taken from the first occurrence)
"""

import argparse
import json
import logging
import os

import matplotlib
matplotlib.use("Agg")          # headless – no display required on GCE VM
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s")
logger = logging.getLogger(__name__)

# Colour scheme
COLOURS = {
    "ego":         "#2196F3",   # blue
    "surrounding": "#FF5722",   # orange-red
    "start":       "#4CAF50",   # green  (start marker)
    "end":         "#F44336",   # red    (end marker)
}

SCENARIO_TITLES = {
    "car_following": "Car-Following",
    "lane_change":   "Lane Change",
    "overtaking":    "Overtaking",
}


# ═════════════════════════════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════════════════════════════

def _parse_args():
    p = argparse.ArgumentParser(description="Visualise NGSIM scenario samples.")
    p.add_argument("--csv",        default="output/scenario_samples.csv",
                   help="Path to scenario_samples.csv produced by the pipeline.")
    p.add_argument("--output_dir", default="output/plots",
                   help="Directory to write PNG files.")
    p.add_argument("--max_per_type", type=int, default=3,
                   help="Maximum trajectory plots to generate per scenario type.")
    return p.parse_args()


# ═════════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════════

def main():
    args = _parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    logger.info("Loading samples from %s", args.csv)
    df = pd.read_csv(args.csv)
    logger.info("Loaded %d samples — types: %s",
                len(df), df["scenario_type"].value_counts().to_dict())

    # 1. Summary bar chart
    _plot_summary(df, args.output_dir)

    # 2. Trajectory plots (first N of each type)
    for stype in ["car_following", "lane_change", "overtaking"]:
        subset = df[df["scenario_type"] == stype].head(args.max_per_type)
        for i, (_, row) in enumerate(subset.iterrows(), 1):
            _plot_trajectory(row, stype, i, args.output_dir)

    logger.info("All plots written to %s/", args.output_dir)


# ═════════════════════════════════════════════════════════════════════════════
# Plot helpers
# ═════════════════════════════════════════════════════════════════════════════

def _plot_summary(df: pd.DataFrame, out_dir: str) -> None:
    """Bar chart of scenario type counts."""
    counts = df["scenario_type"].value_counts().reindex(
        ["car_following", "lane_change", "overtaking"], fill_value=0
    )
    labels = [SCENARIO_TITLES.get(k, k) for k in counts.index]

    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.bar(labels, counts.values,
                  color=["#2196F3", "#4CAF50", "#FF5722"], edgecolor="white",
                  width=0.5)

    for bar, val in zip(bars, counts.values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                str(val), ha="center", va="bottom", fontsize=11, fontweight="bold")

    ax.set_title("Scenario Type Distribution\n(NGSIM US-101, 7:50–8:05 AM)",
                 fontsize=13, fontweight="bold")
    ax.set_ylabel("Number of Samples")
    ax.set_ylim(0, counts.max() * 1.15 + 2)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    out = os.path.join(out_dir, "summary_counts.png")
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved summary chart → %s", out)


def _plot_trajectory(row: pd.Series, stype: str, idx: int,
                     out_dir: str) -> None:
    """
    Plot ego + surrounding vehicle trajectories for one scenario window.
    Parses the JSON ``vehicle_positions`` column.
    """
    try:
        positions = json.loads(row["vehicle_positions"])
    except (json.JSONDecodeError, KeyError):
        logger.warning("Could not parse vehicle_positions for sample %s", row.get("sample_id"))
        return

    ego_pos = positions.get("ego", [])
    if not ego_pos:
        return

    ego_xs = [p[0] for p in ego_pos]
    ego_ys = [p[1] for p in ego_pos]

    fig, ax = plt.subplots(figsize=(8, 5))

    # Ego trajectory
    ax.plot(ego_ys, ego_xs, color=COLOURS["ego"], linewidth=2.5,
            label=f"Ego (ID {row['ego_vehicle_id']})", zorder=3)
    ax.scatter(ego_ys[0],  ego_xs[0],  color=COLOURS["start"], s=80,
               zorder=4, marker="o")
    ax.scatter(ego_ys[-1], ego_xs[-1], color=COLOURS["end"],   s=80,
               zorder=4, marker="s")

    # Surrounding vehicle trajectories
    surround_keys = [k for k in positions if k != "ego"]
    for k in surround_keys:
        pts = positions[k]
        if not pts:
            continue
        sxs = [p[0] for p in pts]
        sys = [p[1] for p in pts]
        ax.plot(sys, sxs, color=COLOURS["surrounding"], linewidth=1.5,
                alpha=0.7, linestyle="--", label=f"Vehicle {k}")
        ax.scatter(sys[0],  sxs[0],  color=COLOURS["start"], s=50, zorder=4,
                   marker="o", alpha=0.7)
        ax.scatter(sys[-1], sxs[-1], color=COLOURS["end"],   s=50, zorder=4,
                   marker="s", alpha=0.7)

    # Annotations
    t_start = row["start_time"]
    t_end   = row["end_time"]
    lane    = row.get("ego_lane", row.get("lane", "?"))
    avg_spd = row.get("average_speed", "?")

    title = (
        f"{SCENARIO_TITLES.get(stype, stype)}  —  "
        f"Ego ID {row['ego_vehicle_id']}\n"
        f"Lane {lane}  |  Avg speed {avg_spd} ft/s  |  "
        f"Window {_fmt_time(t_start)} – {_fmt_time(t_end)}"
    )
    ax.set_title(title, fontsize=11, fontweight="bold")
    ax.set_xlabel("Local Y (ft)  →  direction of travel")
    ax.set_ylabel("Local X (ft)  →  lateral position")
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(linestyle="--", alpha=0.3)

    # Legend
    handles = [
        mpatches.Patch(color=COLOURS["ego"],         label="Ego vehicle"),
        mpatches.Patch(color=COLOURS["surrounding"],  label="Surrounding vehicle(s)"),
        plt.Line2D([0], [0], marker="o", color="w",
                   markerfacecolor=COLOURS["start"], markersize=8, label="Start"),
        plt.Line2D([0], [0], marker="s", color="w",
                   markerfacecolor=COLOURS["end"],   markersize=8, label="End"),
    ]
    ax.legend(handles=handles, loc="upper left", fontsize=9,
              framealpha=0.7)

    out = os.path.join(out_dir, f"{stype}_{idx:02d}_ego{row['ego_vehicle_id']}.png")
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved trajectory plot → %s", out)


def _fmt_time(epoch_ms) -> str:
    """Format epoch-ms timestamp as HH:MM:SS for axis labels."""
    import datetime
    try:
        return datetime.datetime.utcfromtimestamp(float(epoch_ms) / 1000).strftime("%H:%M:%S")
    except Exception:
        return str(epoch_ms)


if __name__ == "__main__":
    main()
