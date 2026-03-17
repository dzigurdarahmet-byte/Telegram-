"""
Генерация графиков для отчётов Telegram-бота
Стиль: GitHub Neon — тёмный фон, голубые акценты
"""

import io
import numpy as np
import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams['font.family'] = ['DejaVu Sans', 'Arial', 'sans-serif']
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
from matplotlib.ticker import FuncFormatter
from matplotlib.patches import Patch
from matplotlib.colors import LinearSegmentedColormap
from datetime import datetime


# ─── Палитра: GitHub Neon ────────────────────────────────

BG_COLOR = "#0d1117"
CARD_COLOR = "#161b22"
TEXT_COLOR = "#f0f6fc"
TEXT_MUTED = "#8b949e"
COLOR_CURRENT = "#58a6ff"
COLOR_PREVIOUS = "#30363d"
COLOR_UP = "#3fb950"
COLOR_DOWN = "#f85149"
GRID_COLOR = "#21262d"

COLOR_DELIVERY = "#f78166"
COLOR_ABC_A = "#3fb950"
COLOR_ABC_B = "#d29922"
COLOR_ABC_C = "#f85149"

WEEKDAY_SHORT = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


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


# ═══════════════════════════════════════════════════════════
# YoY — год к году (существующий)
# ═══════════════════════════════════════════════════════════

def generate_yoy_chart(current: dict, previous: dict, label: str) -> io.BytesIO:
    """PNG с 3 субграфиками: Выручка, Средний чек, Кол-во чеков."""
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
            width=0.50, edgecolor="none", zorder=3,
        )

        for bar, val, c in zip(bars, [prev_val, cur_val], [TEXT_MUTED, TEXT_COLOR]):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + peak * 0.03,
                _fmt_number(val),
                ha="center", va="bottom",
                fontsize=14, fontweight="bold", color=c,
                path_effects=[pe.withStroke(linewidth=4, foreground=BG_COLOR)],
            )

        ax.set_xticks([0, 1])
        ax.set_xticklabels([])
        ax.text(0, -peak * 0.07, "Прошлый год",
                ha="center", va="top", fontsize=10, color=TEXT_MUTED)
        ax.text(1, -peak * 0.07, "Этот год",
                ha="center", va="top", fontsize=11, fontweight="bold", color=TEXT_COLOR)

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

        ax.text(
            0.5, 1.04, title,
            transform=ax.transAxes, ha="center", va="bottom",
            fontsize=13, color=TEXT_MUTED,
        )

        ax.set_ylim(0, peak * 1.35)
        ax.set_xlim(-0.55, 1.55)
        ax.tick_params(axis="y", colors=TEXT_MUTED, labelsize=9)
        ax.tick_params(axis="x", length=0)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: _fmt_number(v)))
        ax.yaxis.grid(True, color=GRID_COLOR, linewidth=0.6, zorder=0)
        ax.set_axisbelow(True)

    fig.legend(
        handles=[
            Patch(facecolor=COLOR_PREVIOUS, label="Прошлый год"),
            Patch(facecolor=COLOR_CURRENT, label="Этот год"),
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


# ═══════════════════════════════════════════════════════════
# 1. ТРЕНД ВЫРУЧКИ
# ═══════════════════════════════════════════════════════════

def generate_revenue_trend(day_data: list, delivery_data: list = None,
                           label: str = "") -> io.BytesIO:
    """Линейный график: выручка по дням (зал + доставка)."""
    if not day_data or len(day_data) < 2:
        return None

    day_data = sorted(day_data, key=lambda x: x["date"])
    dates = [d["date"] for d in day_data]
    hall_rev = [d["revenue"] for d in day_data]

    # Доставка — выровнять по датам зала
    del_rev = None
    if delivery_data:
        del_map = {d["date"]: d["revenue"] for d in delivery_data}
        del_rev = [del_map.get(d, 0) for d in dates]
        if all(v == 0 for v in del_rev):
            del_rev = None

    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor(BG_COLOR)
    ax.set_facecolor(CARD_COLOR)
    for s in ax.spines.values():
        s.set_visible(False)

    x = range(len(dates))

    # Выходные — затенение
    for i, d in enumerate(dates):
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            if dt.weekday() >= 5:
                ax.axvspan(i - 0.5, i + 0.5, alpha=0.04, color="white", zorder=0)
        except ValueError:
            pass

    # Линии
    ax.plot(x, hall_rev, color=COLOR_CURRENT, linewidth=2.5, marker="o",
            markersize=6, label="Зал", zorder=3)

    if del_rev:
        ax.plot(x, del_rev, color=COLOR_DELIVERY, linewidth=2, marker="s",
                markersize=5, linestyle="--", label="Доставка", zorder=3)

    # Среднее зала
    avg_hall = sum(hall_rev) / len(hall_rev) if hall_rev else 0
    ax.axhline(y=avg_hall, color=TEXT_MUTED, linestyle=":", linewidth=1, alpha=0.7, zorder=2)
    ax.text(len(x) - 1, avg_hall, f" {_fmt_number(avg_hall)}",
            fontsize=9, color=TEXT_MUTED, va="bottom")

    # Подписи значений (все если <=7, иначе только min/max)
    if len(hall_rev) <= 7:
        for i, v in enumerate(hall_rev):
            ax.text(i, v + max(hall_rev) * 0.03, _fmt_number(v),
                    ha="center", va="bottom", fontsize=9, color=TEXT_COLOR,
                    path_effects=[pe.withStroke(linewidth=3, foreground=BG_COLOR)])
    else:
        max_i = hall_rev.index(max(hall_rev))
        min_i = hall_rev.index(min(hall_rev))
        for i in (max_i, min_i):
            ax.text(i, hall_rev[i] + max(hall_rev) * 0.03, _fmt_number(hall_rev[i]),
                    ha="center", va="bottom", fontsize=9, color=TEXT_COLOR,
                    path_effects=[pe.withStroke(linewidth=3, foreground=BG_COLOR)])

    # Ось X: дд.мм + день недели
    x_labels = []
    for d in dates:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            x_labels.append(f"{dt.strftime('%d.%m')}\n{WEEKDAY_SHORT[dt.weekday()]}")
        except ValueError:
            x_labels.append(d[5:])
    ax.set_xticks(list(x))
    ax.set_xticklabels(x_labels, fontsize=9, color=TEXT_MUTED)

    ax.tick_params(axis="y", colors=TEXT_MUTED, labelsize=9)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: _fmt_number(v)))
    ax.yaxis.grid(True, color=GRID_COLOR, linewidth=0.6, zorder=0)
    ax.set_axisbelow(True)

    ax.set_title(f"Тренд выручки  \u2022  {label}", fontsize=15,
                 fontweight="bold", color=TEXT_COLOR, pad=15)

    ax.legend(loc="upper left", frameon=False, fontsize=10, labelcolor=TEXT_MUTED)

    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, facecolor=BG_COLOR, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf


# ═══════════════════════════════════════════════════════════
# 2. HEATMAP ЗАГРУЗКИ
# ═══════════════════════════════════════════════════════════

def generate_hourly_heatmap(hour_weekday_data: list, metric: str = "revenue",
                            label: str = "") -> io.BytesIO:
    """Тепловая карта: загрузка по часам и дням недели."""
    if not hour_weekday_data:
        return None

    # Собираем матрицу 7 (дни) × часы
    hours = sorted(set(d["hour"] for d in hour_weekday_data))
    if not hours:
        return None
    h_min, h_max = min(hours), max(hours)
    h_range = list(range(h_min, h_max + 1))

    matrix = np.zeros((7, len(h_range)))
    for d in hour_weekday_data:
        wd = d["weekday"]
        h_idx = d["hour"] - h_min
        if 0 <= wd < 7 and 0 <= h_idx < len(h_range):
            matrix[wd][h_idx] = d.get(metric, 0)

    if matrix.max() == 0:
        return None

    cmap = LinearSegmentedColormap.from_list("neon", [CARD_COLOR, "#1a3a5c", COLOR_CURRENT])

    fig, ax = plt.subplots(figsize=(max(10, len(h_range) * 1.2), 5))
    fig.patch.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    im = ax.imshow(matrix, cmap=cmap, aspect="auto", vmin=0)

    # Числа в ячейках
    peak = matrix.max() if matrix.max() > 0 else 1
    for i in range(7):
        for j in range(len(h_range)):
            val = matrix[i][j]
            if val == 0:
                continue
            if metric == "revenue":
                txt = _fmt_number(val)
            else:
                txt = f"{val:.0f}"
            color = TEXT_COLOR if val > peak * 0.5 else TEXT_MUTED
            weight = "bold" if val > peak * 0.7 else "normal"
            ax.text(j, i, txt, ha="center", va="center",
                    fontsize=8, color=color, fontweight=weight)

    ax.set_xticks(range(len(h_range)))
    ax.set_xticklabels([f"{h}:00" for h in h_range], fontsize=9, color=TEXT_MUTED)
    ax.set_yticks(range(7))
    ax.set_yticklabels(WEEKDAY_SHORT, fontsize=10, color=TEXT_MUTED)

    # Разделитель перед выходными
    ax.axhline(y=3.5, color=GRID_COLOR, linewidth=1.5)

    metric_label = "выручка" if metric == "revenue" else "заказы"
    ax.set_title(f"Загрузка по часам ({metric_label})  \u2022  {label}",
                 fontsize=15, fontweight="bold", color=TEXT_COLOR, pad=15)

    cbar = plt.colorbar(im, ax=ax, fraction=0.02, pad=0.04)
    cbar.ax.tick_params(colors=TEXT_MUTED, labelsize=8)

    for s in ax.spines.values():
        s.set_visible(False)

    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, facecolor=BG_COLOR, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf


# ═══════════════════════════════════════════════════════════
# 3. ABC ПУЗЫРЬКОВАЯ ДИАГРАММА
# ═══════════════════════════════════════════════════════════

def generate_abc_bubble(dish_data: list, label: str = "") -> io.BytesIO:
    """Пузырьковая диаграмма ABC-анализа блюд."""
    dishes = [d for d in dish_data if d.get("qty", 0) > 0 and d.get("revenue", 0) > 0]
    if len(dishes) < 5:
        return None

    # ABC-классификация
    dishes = sorted(dishes, key=lambda x: x["revenue"], reverse=True)
    total_rev = sum(d["revenue"] for d in dishes)
    cumulative = 0
    for d in dishes:
        cumulative += d["revenue"]
        pct = cumulative / total_rev if total_rev > 0 else 1
        if pct <= 0.80:
            d["abc"] = "A"
        elif pct <= 0.95:
            d["abc"] = "B"
        else:
            d["abc"] = "C"

    fig, ax = plt.subplots(figsize=(14, 8))
    fig.patch.set_facecolor(BG_COLOR)
    ax.set_facecolor(CARD_COLOR)
    for s in ax.spines.values():
        s.set_visible(False)

    abc_colors = {"A": COLOR_ABC_A, "B": COLOR_ABC_B, "C": COLOR_ABC_C}

    # Размер пузырька = средний чек (нормализованный)
    avg_prices = [d["revenue"] / d["qty"] for d in dishes]
    max_price = max(avg_prices) if avg_prices else 1
    min_size, max_size = 50, 500

    for cat in ["C", "B", "A"]:  # Рисуем A поверх
        subset = [d for d in dishes if d["abc"] == cat]
        if not subset:
            continue
        x_vals = [d["qty"] for d in subset]
        y_vals = [d["revenue"] for d in subset]
        sizes = [
            min_size + (d["revenue"] / d["qty"] / max_price) * (max_size - min_size)
            for d in subset
        ]
        ax.scatter(x_vals, y_vals, s=sizes, c=abc_colors[cat],
                   alpha=0.7, edgecolors="none", zorder=3, label=cat)

    # Подписи топ-10
    for d in dishes[:10]:
        name = d["name"]
        if len(name) > 20:
            name = name[:18] + ".."
        ax.annotate(
            name, (d["qty"], d["revenue"]),
            xytext=(10, 10), textcoords="offset points",
            fontsize=8, color=TEXT_COLOR, alpha=0.9,
            path_effects=[pe.withStroke(linewidth=2, foreground=BG_COLOR)],
        )

    ax.set_xlabel("Количество продаж (шт)", fontsize=11, color=TEXT_MUTED)
    ax.set_ylabel("Выручка (руб)", fontsize=11, color=TEXT_MUTED)
    ax.tick_params(axis="both", colors=TEXT_MUTED, labelsize=9)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: _fmt_number(v)))
    ax.xaxis.grid(True, color=GRID_COLOR, linewidth=0.6, zorder=0)
    ax.yaxis.grid(True, color=GRID_COLOR, linewidth=0.6, zorder=0)
    ax.set_axisbelow(True)

    ax.set_title(f"ABC-анализ блюд  \u2022  {label}",
                 fontsize=15, fontweight="bold", color=TEXT_COLOR, pad=15)

    ax.legend(
        handles=[
            Patch(facecolor=COLOR_ABC_A, label="A (80% выручки)"),
            Patch(facecolor=COLOR_ABC_B, label="B (15%)"),
            Patch(facecolor=COLOR_ABC_C, label="C (5%)"),
        ],
        loc="upper right", frameon=False, fontsize=10, labelcolor=TEXT_MUTED,
    )

    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, facecolor=BG_COLOR, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf
