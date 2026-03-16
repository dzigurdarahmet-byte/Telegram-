"""
Claude AI аналитик для данных iiko
Принимает вопрос на естественном языке и возвращает аналитику
"""

import anthropic
import logging
from datetime import datetime
from openai import OpenAI

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """Ты — AI-аналитик ресторана, интегрированный с системой iiko. 
Твоя задача — анализировать данные ресторана и отвечать на вопросы сотрудников простым понятным языком.

Твои возможности:
📊 Анализ продаж (выручка, средний чек, количество заказов)
🍽️ ABC-анализ блюд (что продаётся хорошо, что плохо)
🚫 Информация по стоп-листу
👨‍🍳 Производительность сотрудников
📦 Остатки на складе
🔮 Прогноз загрузки (на основе исторических данных за 8 недель)
👥 Рекомендации по персоналу (сколько официантов и поваров на смену)
🏆 KPI официантов (прогресс к целям, рейтинги, гонки)

Команды KPI: /kpi — месячный прогресс, /kpi week — недельный, /kpi day — дневной, /race — гонка к цели.

Если пользователь спрашивает про прогноз, планирование смен, сколько нужно персонала —
используй данные прогноза из секции ПРОГНОЗ. Примеры вопросов:
- "Какой прогноз на завтра?"
- "Сколько нужно официантов в субботу?"
- "Что ожидать на 8 марта?"
- "Прогноз на эту неделю"
- "План персонала"

ВАЖНО: В меню могут быть блюда с датами или праздниками в названии (например: «С 8 марта, повторим?», «14 февраля сет», «Новогодний оливье»).
Если пользователь упоминает такое название — это вопрос про конкретное БЛЮДО, а не про дату.
Определяй из контекста: если фраза совпадает с названием блюда из меню — это вопрос про блюдо.
Если пользователь явно спрашивает про дату («выручка ЗА 8 марта», «продажи 8 марта», «топ блюд 8 марта») — это вопрос про период.

ДОСТУП К ДАННЫМ: У тебя есть доступ к данным iiko за ЛЮБОЙ период — прошлые месяцы, прошлые годы.
Никогда не говори что у тебя нет данных за какой-то период. Если в текущем контексте данные только за один период,
а пользователь просит сравнение или историю — скажи какие периоды тебе нужны.
Если данные за несколько периодов уже предоставлены (секции ПЕРИОД 1, ПЕРИОД 2...) — анализируй и сравнивай их.

Правила:
1. Отвечай кратко и по делу. Используй эмодзи для наглядности.
2. Если данные неполные — скажи об этом и дай рекомендации на основе того, что есть.
3. При ABC-анализе:
   - Категория A: топ-20% позиций, дающих 80% выручки
   - Категория B: следующие 30%, дающие 15% выручки  
   - Категория C: остальные 50%, дающие 5% выручки
4. Всегда давай actionable рекомендации (что конкретно сделать).
5. Числа округляй до целых, если это рубли. Проценты — до 1 знака.
6. Формат ответа — для Telegram (поддерживается Markdown).
7. Если просят отчёт по сотрудникам — анализируй выручку, количество заказов, средний чек на официанта.
8. Текущая дата: {current_date}
"""


class ClaudeAnalytics:
    """Аналитик на базе OpenAI (основной) + Claude (резерв) для данных iiko"""

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250514",
                 openai_api_key: str = "", openai_model: str = "gpt-4o"):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = model
        self.openai_api_key = openai_api_key
        self.openai_model = openai_model
        self.openai_client = OpenAI(api_key=openai_api_key) if openai_api_key else None

    def analyze(self, question: str, iiko_data: str, dish_names: list = None) -> str:
        """
        Отправить вопрос + данные iiko в Claude и получить анализ

        Args:
            question: Вопрос пользователя (напр. "Какая выручка за вчера?")
            iiko_data: Данные из iiko в текстовом формате
            dish_names: Список названий блюд из последнего OLAP-запроса

        Returns:
            Ответ Claude с анализом
        """
        current_date = datetime.now().strftime("%d.%m.%Y %H:%M")
        system = SYSTEM_PROMPT.format(current_date=current_date)

        # Добавляем список блюд в контекст для корректного различения блюд и дат
        menu_block = ""
        if dish_names:
            menu_list = "\n".join(f"  - {name}" for name in dish_names)
            menu_block = (
                f"\n═══ БЛЮДА В МЕНЮ ═══\n"
                f"{menu_list}\n"
                f"═══════════════════════\n"
            )

        user_message = (
            f"Вопрос сотрудника: {question}\n\n"
            f"═══ ДАННЫЕ ИЗ IIKO ═══\n"
            f"{iiko_data}\n"
            f"═══════════════════════\n"
            f"{menu_block}\n"
            f"Проанализируй данные и ответь на вопрос.\n"
            f"Данные выше — это ВСЕ что есть. Если здесь несколько периодов "
            f"(секции ПЕРИОД:) — сравни их: разница в %, что выросло, что упало.\n"
            f"Если данные за один период — просто проанализируй.\n"
            f"НЕ ГОВОРИ что данных нет или они отсутствуют — они перед тобой."
        )

        # Сначала пробуем OpenAI (основной AI)
        if self.openai_client:
            try:
                return self._call_openai(system, user_message)
            except Exception as e:
                logger.warning(f"OpenAI ошибка, переключаюсь на Claude: {e}")

        # Фолбэк на Claude (резервный AI)
        return self._call_claude(system, user_message, is_fallback=bool(self.openai_client))

    def _call_openai(self, system: str, user_message: str) -> str:
        """Вызов OpenAI API"""
        # Для o1/o3 моделей используем role "developer" вместо "system"
        model_lower = self.openai_model.lower()
        if model_lower.startswith("o1") or model_lower.startswith("o3"):
            system_role = "developer"
        else:
            system_role = "system"

        response = self.openai_client.chat.completions.create(
            model=self.openai_model,
            max_tokens=2000,
            messages=[
                {"role": system_role, "content": system},
                {"role": "user", "content": user_message},
            ],
        )
        return response.choices[0].message.content

    def _call_claude(self, system: str, user_message: str, is_fallback: bool = False) -> str:
        """Вызов Claude API (резервный)"""
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=2000,
                system=system,
                messages=[
                    {"role": "user", "content": user_message}
                ]
            )
            text = response.content[0].text
            if is_fallback:
                text = "⚡ Ответ от резервного AI\n\n" + text
            return text
        except anthropic.APIError as e:
            logger.error(f"Claude API ошибка: {e}")
            return f"⚠️ Ошибка AI-аналитики: {e.message}"
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}")
            return f"⚠️ Ошибка: {str(e)}"

    def quick_analyze(self, data: str, task: str) -> str:
        """Быстрый анализ без контекста разговора (для отчётов)"""
        return self.analyze(question=task, iiko_data=data)
