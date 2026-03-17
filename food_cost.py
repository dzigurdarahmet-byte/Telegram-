"""
Food Cost мониторинг — анализ маржинальности блюд.
Источник: OLAP iiko Server с полями себестоимости + выручки.
"""

import logging

logger = logging.getLogger(__name__)


def _safe_float(val) -> float:
    try:
        return float(val) if val else 0.0
    except (ValueError, TypeError):
        return 0.0


class FoodCostAnalyzer:
    """Анализ маржинальности блюд из данных iiko"""

    def __init__(self, iiko_server):
        self.iiko_server = iiko_server

    async def get_food_cost_data(self, date_from: str, date_to: str) -> dict:
        """Запросить OLAP с себестоимостью."""
        try:
            rows = await self.iiko_server._olap_request(
                date_from, date_to,
                group_fields=["DishName", "DishGroup"],
                aggregate_fields=[
                    "DishDiscountSumInt",
                    "DishSumInt",
                    "DishAmountInt",
                    "ProductCostBase.ProductCost",
                ]
            )

            if not rows:
                return {"error": "OLAP вернул 0 строк", "rows": []}

            sample = rows[0] if rows else {}
            cost_field = None
            for field in ["ProductCostBase.ProductCost", "Себестоимость",
                          "ProductCost", "CostPrice", "Себестоимость базовая"]:
                if field in sample:
                    cost_field = field
                    break

            if not cost_field:
                logger.warning("Нет ProductCost в OLAP, пробуем ProductCostBase.OneItem")
                rows = await self.iiko_server._olap_request(
                    date_from, date_to,
                    group_fields=["DishName", "DishGroup"],
                    aggregate_fields=[
                        "DishDiscountSumInt",
                        "DishAmountInt",
                        "ProductCostBase.OneItem",
                    ]
                )
                sample = rows[0] if rows else {}
                for field in ["ProductCostBase.OneItem", "Себестоимость порции",
                              "CostPerItem", "OneItemCost"]:
                    if field in sample:
                        cost_field = field
                        break

            return {
                "rows": rows,
                "cost_field": cost_field,
                "has_cost": cost_field is not None,
                "fields_available": list(sample.keys()) if sample else [],
            }

        except Exception as e:
            logger.error(f"Food cost OLAP: {e}")
            return {"error": str(e), "rows": []}

    def analyze(self, data: dict) -> list:
        """Анализировать данные и вернуть список блюд с маржой."""
        rows = data.get("rows", [])
        cost_field = data.get("cost_field")
        has_cost = data.get("has_cost", False)

        dishes = []
        for row in rows:
            name = row.get("DishName") or row.get("Блюдо") or "?"
            group = row.get("DishGroup") or row.get("Группа блюда") or "?"
            revenue = _safe_float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой"))
            quantity = _safe_float(row.get("DishAmountInt") or row.get("Количество блюд"))

            if has_cost and cost_field:
                raw_cost = _safe_float(row.get(cost_field))
                if "OneItem" in (cost_field or "") or "порции" in (cost_field or "").lower():
                    cost = raw_cost * quantity
                else:
                    cost = raw_cost
            else:
                cost = 0

            profit = revenue - cost
            margin_pct = (profit / revenue * 100) if revenue > 0 else 0

            dishes.append({
                "name": name,
                "group": group,
                "revenue": revenue,
                "quantity": quantity,
                "cost": cost,
                "profit": profit,
                "margin_pct": margin_pct,
                "revenue_per_unit": revenue / quantity if quantity > 0 else 0,
                "cost_per_unit": cost / quantity if quantity > 0 else 0,
                "profit_per_unit": profit / quantity if quantity > 0 else 0,
            })

        return dishes

    def format_for_ai(self, dishes: list, has_cost: bool) -> str:
        """Форматировать данные для передачи в AI"""
        if not dishes:
            return "⚠️ Нет данных по блюдам"

        if has_cost:
            sorted_dishes = sorted(dishes, key=lambda x: x["profit"], reverse=True)
        else:
            sorted_dishes = sorted(dishes, key=lambda x: x["revenue"], reverse=True)

        lines = []

        if has_cost:
            lines.append("═══ МАРЖИНАЛЬНОСТЬ БЛЮД (с себестоимостью) ═══")
            lines.append("Блюдо | Кол-во | Выручка | Себест. | Прибыль | Маржа%")
            lines.append("-" * 70)

            for d in sorted_dishes:
                if d["revenue"] > 0:
                    lines.append(
                        f"  {d['name']} | {d['quantity']:.0f} шт | "
                        f"{d['revenue']:.0f} руб. | {d['cost']:.0f} руб. | "
                        f"{d['profit']:.0f} руб. | {d['margin_pct']:.0f}%"
                    )

            total_revenue = sum(d["revenue"] for d in sorted_dishes)
            total_cost = sum(d["cost"] for d in sorted_dishes)
            total_profit = total_revenue - total_cost
            total_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0

            lines.append("-" * 70)
            lines.append(
                f"  ИТОГО | — | {total_revenue:.0f} руб. | {total_cost:.0f} руб. | "
                f"{total_profit:.0f} руб. | {total_margin:.0f}%"
            )

            # Группировка по маржинальности
            lines.append("")
            lines.append("═══ ГРУППИРОВКА ПО МАРЖЕ ═══")

            high = [d for d in sorted_dishes if d["margin_pct"] >= 70 and d["revenue"] > 0]
            medium = [d for d in sorted_dishes if 40 <= d["margin_pct"] < 70 and d["revenue"] > 0]
            low = [d for d in sorted_dishes if 0 < d["margin_pct"] < 40 and d["revenue"] > 0]
            negative = [d for d in sorted_dishes if d["margin_pct"] <= 0 and d["cost"] > 0]

            if high:
                lines.append(f"\n🟢 ВЫСОКАЯ МАРЖА (>70%): {len(high)} блюд")
                for d in high[:10]:
                    lines.append(f"  {d['name']} — маржа {d['margin_pct']:.0f}%, прибыль {d['profit']:.0f} руб.")

            if medium:
                lines.append(f"\n🟡 СРЕДНЯЯ МАРЖА (40-70%): {len(medium)} блюд")
                for d in medium[:10]:
                    lines.append(f"  {d['name']} — маржа {d['margin_pct']:.0f}%, прибыль {d['profit']:.0f} руб.")

            if low:
                lines.append(f"\n🔴 НИЗКАЯ МАРЖА (<40%): {len(low)} блюд")
                for d in low[:10]:
                    lines.append(f"  {d['name']} — маржа {d['margin_pct']:.0f}%, прибыль {d['profit']:.0f} руб.")

            if negative:
                lines.append(f"\n⛔ УБЫТОЧНЫЕ: {len(negative)} блюд")
                for d in negative[:5]:
                    lines.append(f"  {d['name']} — маржа {d['margin_pct']:.0f}%, убыток {abs(d['profit']):.0f} руб.")

            # Инсайты
            lines.append("")
            lines.append("═══ КЛЮЧЕВЫЕ ИНСАЙТЫ ═══")

            stars = sorted(
                [d for d in sorted_dishes if d["margin_pct"] >= 60 and d["quantity"] >= 5],
                key=lambda x: x["profit"], reverse=True
            )[:5]
            if stars:
                lines.append("⭐ ЗВЁЗДЫ (высокая маржа + популярность):")
                for d in stars:
                    lines.append(f"  {d['name']} — {d['quantity']:.0f} шт, маржа {d['margin_pct']:.0f}%, прибыль {d['profit']:.0f}")

            puzzles = sorted(
                [d for d in sorted_dishes if d["margin_pct"] >= 60 and 0 < d["quantity"] < 5],
                key=lambda x: x["margin_pct"], reverse=True
            )[:5]
            if puzzles:
                lines.append("🔍 СКРЫТЫЕ ВОЗМОЖНОСТИ (высокая маржа, мало продаж):")
                for d in puzzles:
                    lines.append(f"  {d['name']} — {d['quantity']:.0f} шт, маржа {d['margin_pct']:.0f}%")

            traps = sorted(
                [d for d in sorted_dishes if d["margin_pct"] < 40 and d["quantity"] >= 10 and d["cost"] > 0],
                key=lambda x: x["quantity"], reverse=True
            )[:5]
            if traps:
                lines.append("⚠️ ЛОВУШКИ (популярные, но низкая маржа):")
                for d in traps:
                    lines.append(f"  {d['name']} — {d['quantity']:.0f} шт, маржа {d['margin_pct']:.0f}%")

        else:
            lines.append("═══ ПРОДАЖИ ПО БЛЮДАМ (себестоимость недоступна) ═══")
            lines.append("⚠️ Техкарты не настроены в iiko или поле себестоимости недоступно.\n")
            for d in sorted_dishes[:30]:
                if d["revenue"] > 0:
                    lines.append(f"  {d['name']} | {d['quantity']:.0f} шт | {d['revenue']:.0f} руб. | {d['group']}")

        return "\n".join(lines)
