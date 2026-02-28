"""
Генерация графиков год-к-году (YoY) для отчётов /today и /month
"""

import io
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


def _fmt_number(value: float) -> str:
    """Форматирование числа: 1234567 → '1.23M', 12345 → '12.3K'"""
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
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
    Генерирует PNG-изображение с 3 субграфиками (горизонтально):
      1. Выручка (текущий год / прошлый год)
      2. Средний чек
      3. Количество чеков

    current / previous — dict с ключами: revenue, orders, avg_check
    label — заголовок периода (напр. "Сегодня 28.02")

    Возвращает BytesIO с PNG.
    """
    metrics = [
        ("Выручка, руб.", "revenue"),
        ("Средний чек, руб.", "avg_check"),
        ("Кол-во чеков", "orders"),
    ]

    fig, axes = plt.subplots(1, 3, figsize=(14, 5))
    fig.suptitle(f"Год к году: {label}", fontsize=14, fontweight="bold", y=0.98)

    color_current = "#2ecc71"
    color_previous = "#95a5a6"

    for ax, (title, key) in zip(axes, metrics):
        cur_val = current.get(key, 0)
        prev_val = previous.get(key, 0)

        bars = ax.bar(
            ["Прошлый\nгод", "Этот\nгод"],
            [prev_val, cur_val],
            color=[color_previous, color_current],
            width=0.5,
            edgecolor="white",
            linewidth=1.5,
        )

        # Подписи значений на столбцах
        for bar, val in zip(bars, [prev_val, cur_val]):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height(),
                _fmt_number(val),
                ha="center", va="bottom",
                fontsize=11, fontweight="bold",
            )

        # Процент изменения
        pct = _pct_change(cur_val, prev_val)
        pct_color = "#27ae60" if cur_val >= prev_val else "#e74c3c"
        ax.set_title(f"{title}\n{pct}", fontsize=11, color=pct_color)

        ax.set_ylim(0, max(cur_val, prev_val, 1) * 1.25)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: _fmt_number(v)))
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    plt.tight_layout(rect=[0, 0, 1, 0.93])

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf
