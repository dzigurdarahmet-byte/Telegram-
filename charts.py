"""
Генерация графиков год-к-году (YoY) для отчётов /today и /month
"""

import io
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
from matplotlib.ticker import FuncFormatter


# ─── Палитра и стиль ──────────────────────────────────────

BG_COLOR = "#0f1117"
CARD_COLOR = "#1a1d27"
TEXT_COLOR = "#e8e8e8"
TEXT_MUTED = "#7f8694"
COLOR_CURRENT = "#6c5ce7"
COLOR_PREVIOUS = "#2d3045"
COLOR_UP = "#00d68f"
COLOR_DOWN = "#ff6b6b"
GRID_COLOR = "#252836"


def _fmt_number(value: float) -> str:
    """Форматирование числа: 1234567 → '1.23M', 12345 → '12.3K'"""
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs(value) >= 10_000:
        return f"{value / 1_000:.1f}K"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{value:.0f}"


def _pct_change(current: float, previous: float) -> str:
    """Процент изменения: +12% / -5% / н/д"""
    if previous == 0:
        return "+∞%" if current > 0 else "—"
    pct = (current - previous) / previous * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.0f}%"


def generate_yoy_chart(current: dict, previous: dict, label: str) -> io.BytesIO:
    """
    Генерирует современный PNG-график с 3 субграфиками (горизонтально):
      1. Выручка (текущий год / прошлый год)
      2. Средний чек
      3. Количество чеков

    current / previous — dict с ключами: revenue, orders, avg_check
    label — заголовок периода (напр. "Сегодня 28.02")

    Возвращает BytesIO с PNG.
    """
    metrics = [
        ("Выручка", "revenue", " \u20bd"),
        ("Средний чек", "avg_check", " \u20bd"),
        ("Кол-во чеков", "orders", ""),
    ]

    fig, axes = plt.subplots(1, 3, figsize=(15, 5.5))
    fig.patch.set_facecolor(BG_COLOR)

    # Заголовок
    fig.text(
        0.5, 0.96, f"Год к году  \u2022  {label}",
        ha="center", va="top",
        fontsize=16, fontweight="bold", color=TEXT_COLOR,
    )

    for ax, (title, key, suffix) in zip(axes, metrics):
        ax.set_facecolor(CARD_COLOR)
        for spine in ax.spines.values():
            spine.set_visible(False)

        cur_val = current.get(key, 0)
        prev_val = previous.get(key, 0)

        x_pos = [0, 1]
        bar_colors = [COLOR_PREVIOUS, COLOR_CURRENT]

        bars = ax.bar(
            x_pos,
            [prev_val, cur_val],
            color=bar_colors,
            width=0.55,
            edgecolor="none",
            zorder=3,
        )

        # Скруглённый вид — градиентная полоска сверху
        for bar, color in zip(bars, bar_colors):
            bar.set_alpha(0.85)

        # Подписи значений над столбцами
        for bar, val in zip(bars, [prev_val, cur_val]):
            y = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                y + max(cur_val, prev_val, 1) * 0.03,
                _fmt_number(val),
                ha="center", va="bottom",
                fontsize=13, fontweight="bold", color=TEXT_COLOR,
                path_effects=[pe.withStroke(linewidth=3, foreground=CARD_COLOR)],
            )

        # Подписи под столбцами
        ax.set_xticks(x_pos)
        ax.set_xticklabels(
            [f"{prev_val and str(int(cur_val) - int(cur_val)) or ''}", ""],
            fontsize=1, color=BG_COLOR,
        )
        # Рисуем кастомные подписи
        ax.text(0, -max(cur_val, prev_val, 1) * 0.08,
                "Прошлый год", ha="center", va="top",
                fontsize=10, color=TEXT_MUTED)
        ax.text(1, -max(cur_val, prev_val, 1) * 0.08,
                "Этот год", ha="center", va="top",
                fontsize=10, color=TEXT_COLOR)

        # Процент изменения — бейдж
        pct_text = _pct_change(cur_val, prev_val)
        is_up = cur_val >= prev_val
        pct_color = COLOR_UP if is_up else COLOR_DOWN
        arrow = "\u25b2" if is_up else "\u25bc"

        ax.text(
            0.5, 1.12, f"{arrow} {pct_text}",
            transform=ax.transAxes,
            ha="center", va="bottom",
            fontsize=13, fontweight="bold", color=pct_color,
            bbox=dict(
                boxstyle="round,pad=0.3",
                facecolor=pct_color + "1a",
                edgecolor=pct_color + "40",
                linewidth=1.2,
            ),
        )

        # Заголовок метрики
        ax.text(
            0.5, 1.02, title,
            transform=ax.transAxes,
            ha="center", va="bottom",
            fontsize=12, fontweight="medium", color=TEXT_MUTED,
        )

        # Оси
        y_max = max(cur_val, prev_val, 1) * 1.30
        ax.set_ylim(0, y_max)
        ax.set_xlim(-0.6, 1.6)
        ax.tick_params(axis="y", colors=TEXT_MUTED, labelsize=9)
        ax.tick_params(axis="x", length=0)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: _fmt_number(v)))

        # Горизонтальная сетка
        ax.yaxis.grid(True, color=GRID_COLOR, linewidth=0.5, zorder=0)
        ax.set_axisbelow(True)

    # Легенда внизу
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=COLOR_PREVIOUS, alpha=0.85, label="Прошлый год"),
        Patch(facecolor=COLOR_CURRENT, alpha=0.85, label="Этот год"),
    ]
    fig.legend(
        handles=legend_elements,
        loc="lower center",
        ncol=2,
        frameon=False,
        fontsize=11,
        labelcolor=TEXT_MUTED,
        handlelength=1.5,
        handleheight=1,
        borderpad=0.3,
    )

    plt.tight_layout(rect=[0, 0.06, 1, 0.90])

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, facecolor=BG_COLOR, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf
