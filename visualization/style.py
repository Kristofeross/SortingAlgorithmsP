import matplotlib.pyplot as plt
import math

from visualization.config import (
    GRID_ALPHA,
    TITLE_SIZE,
    LABEL_SIZE,
    TICK_SIZE,
    LEGEND_SIZE,
    LINE_WIDTH,
    MARKER_SIZE,
    DPI,
)


def initialize_matplotlib() -> None:
    plt.rcParams.update({
        "figure.autolayout": True,
        "axes.titlesize": TITLE_SIZE,
        "axes.labelsize": LABEL_SIZE,
        "xtick.labelsize": TICK_SIZE,
        "ytick.labelsize": TICK_SIZE,
        "legend.fontsize": LEGEND_SIZE,
        "axes.grid": True,
        "grid.alpha": GRID_ALPHA,
        "lines.linewidth": LINE_WIDTH,
        "lines.markersize": MARKER_SIZE,
        "savefig.dpi": DPI,
        "axes.spines.top": False,
        "axes.spines.right": False,
    })


def apply_plot_style(ax) -> None:
    ax.grid(True, alpha=GRID_ALPHA,)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    handles, labels = ax.get_legend_handles_labels()

    if handles:
        ax.legend(fontsize=LEGEND_SIZE, frameon=True,)


def rotate_x_labels(ax, rotation: int = 20,) -> None:
    for label in ax.get_xticklabels():
        label.set_rotation(rotation)
        label.set_ha("right")


def use_log_scale_x(ax) -> None:
    ax.set_xscale("log")


def use_log_scale_y(ax) -> None:
    ax.set_yscale("log")


def use_log_scale(ax) -> None:
    ax.set_xscale("log")
    ax.set_yscale("log")


def format_number(value) -> str:
    return f"{value:,}".replace(",", " ")


def set_clean_ticks(ax, values, axis: str = "x") -> None:
    values = sorted(set(values))
    labels = [format_number(v) for v in values]

    target_axis = ax.xaxis if axis == "x" else ax.yaxis

    target_axis.set_ticks(values)
    target_axis.set_ticklabels(labels)
    target_axis.set_minor_locator(plt.NullLocator())


def plain_log_tick_label(value, _pos=None) -> str:
    if value == 0:
        return "0"

    if value >= 1:
        return f"{value:g}"

    exponent = math.floor(math.log10(abs(value)))
    decimals = max(-exponent, 0)
    return f"{value:.{decimals}f}"


def format_log_axis_plain(ax, axis: str = "y") -> None:
    target_axis = ax.yaxis if axis == "y" else ax.xaxis
    target_axis.set_major_formatter(plt.FuncFormatter(plain_log_tick_label))
    target_axis.set_minor_formatter(plt.NullFormatter())