"""
Генерация графиков год-к-году (YoY) для отчётов /today и /month
Стиль: Warm Coral — тёмно-фиолетовый фон, коралловые столбцы с золотой рамкой
"""

import io
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
from matplotlib.ticker import FuncFormatter
from matplotlib.patches import Patch


# ─── Палитра: Warm Coral ─────────────────────────────────

BG_COLOR = "#1a1423"
CARD_COLOR = "#231c30"
TEXT_COLOR = "#f8f0e3"
TEXT_MUTED = "#a89bb5"
COLOR_CURRENT = "#ff6b6b"
COLOR_PREVIOUS = "#2a2440"
COLOR_UP = "#6bcb77"
COLOR_DOWN = "#ff6b6b"
GRID_COLOR = "#2a2440"
EDGE_CURRENT = "#ffd93d"


def _fmt_number(value: float) -> str:
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{value:.0f}"


def _pct_change(current: float, previous: float) -> str:
    if previous == 0:
        return "+\u221e%" if current > 0 else "\u2014"
    pct = (current - previous) / previous * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.0f}%"


def generate_yoy_chart(current: dict, previous: dict, label: str) -> io.BytesIO:
    """
    PNG с 3 субграфиками: Выручка, Средний чек, Кол-во чеков.
    current / previous — {revenue, orders, avg_check}
    """
    metrics = [
        ("Выручка", "revenue"),
        ("Средний чек", "avg_check"),
        ("Кол-во чеков", "orders"),
    ]

    fig, axes = plt.subplots(1, 3, figsize=(15, 6))
    fig.patch.set_facecolor(BG_COLOR)

    fig.text(
        0.5, 0.97, f"Год к году  \u2022  {label}",
        ha="center", va="top",
        fontsize=18, fontweight="bold", color=TEXT_COLOR,
    )

    for ax, (title, key) in zip(axes, metrics):
        ax.set_facecolor(CARD_COLOR)
        for s in ax.spines.values():
            s.set_visible(False)

        cur_val = current.get(key, 0)
        prev_val = previous.get(key, 0)
        peak = max(cur_val, prev_val, 1)

        bars = ax.bar(
            [0, 1], [prev_val, cur_val],
            color=[COLOR_PREVIOUS, COLOR_CURRENT],
            width=0.50,
            edgecolor=["none", EDGE_CURRENT],
            linewidth=[0, 2],
            zorder=3,
        )

        # Подписи значений
        for bar, val, c in zip(bars, [prev_val, cur_val], [TEXT_MUTED, TEXT_COLOR]):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + peak * 0.03,
                _fmt_number(val),
                ha="center", va="bottom",
                fontsize=14, fontweight="bold", color=c,
                path_effects=[pe.withStroke(linewidth=4, foreground=BG_COLOR)],
            )

        # Подписи X
        ax.set_xticks([0, 1])
        ax.set_xticklabels([])
        ax.text(0, -peak * 0.07, "Прошлый год",
                ha="center", va="top", fontsize=10, color=TEXT_MUTED)
        ax.text(1, -peak * 0.07, "Этот год",
                ha="center", va="top", fontsize=11, fontweight="bold", color=TEXT_COLOR)

        # Бейдж процента
        pct_text = _pct_change(cur_val, prev_val)
        is_up = cur_val >= prev_val
        pct_color = COLOR_UP if is_up else COLOR_DOWN
        arrow = "\u25b2" if is_up else "\u25bc"

        ax.text(
            0.5, 1.14, f"{arrow} {pct_text}",
            transform=ax.transAxes, ha="center", va="bottom",
            fontsize=14, fontweight="bold", color=pct_color,
            bbox=dict(
                boxstyle="round,pad=0.35",
                facecolor=pct_color + "20",
                edgecolor=pct_color + "60",
                linewidth=1.5,
            ),
        )

        # Заголовок метрики
        ax.text(
            0.5, 1.04, title,
            transform=ax.transAxes, ha="center", va="bottom",
            fontsize=13, color=TEXT_MUTED,
        )

        # Оси и сетка
        ax.set_ylim(0, peak * 1.35)
        ax.set_xlim(-0.55, 1.55)
        ax.tick_params(axis="y", colors=TEXT_MUTED, labelsize=9)
        ax.tick_params(axis="x", length=0)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: _fmt_number(v)))
        ax.yaxis.grid(True, color=GRID_COLOR, linewidth=0.6, zorder=0)
        ax.set_axisbelow(True)

    # Легенда
    fig.legend(
        handles=[
            Patch(facecolor=COLOR_PREVIOUS, label="Прошлый год"),
            Patch(facecolor=COLOR_CURRENT, edgecolor=EDGE_CURRENT, linewidth=2, label="Этот год"),
        ],
        loc="lower center", ncol=2, frameon=False,
        fontsize=11, labelcolor=TEXT_MUTED,
        handlelength=1.5, handleheight=1, borderpad=0.3,
    )

    plt.tight_layout(rect=[0, 0.06, 1, 0.88])

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, facecolor=BG_COLOR, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf
