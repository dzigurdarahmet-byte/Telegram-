"""
Голосовой модуль: STT (распознавание) и TTS (синтез) через OpenAI API.

STT: Whisper — отправляем .ogg файл, получаем текст.
TTS: OpenAI TTS — отправляем текст, получаем opus аудио.
"""

import io
import logging
import tempfile
import os
import re
from typing import Optional

from openai import OpenAI

logger = logging.getLogger(__name__)


class VoiceProcessor:
    """Распознавание и синтез речи через OpenAI"""

    def __init__(self, api_key: str,
                 stt_model: str = "whisper-1",
                 tts_model: str = "tts-1",
                 tts_voice: str = "onyx"):
        self.client = OpenAI(api_key=api_key)
        self.stt_model = stt_model
        self.tts_model = tts_model
        self.tts_voice = tts_voice

    async def speech_to_text(self, audio_bytes: bytes, filename: str = "voice.ogg") -> str:
        """Распознать голосовое сообщение в текст."""
        tmp_path = None
        try:
            suffix = os.path.splitext(filename)[1] or ".ogg"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(audio_bytes)
                tmp_path = tmp.name

            with open(tmp_path, "rb") as audio_file:
                response = self.client.audio.transcriptions.create(
                    model=self.stt_model,
                    file=audio_file,
                    language="ru",
                    response_format="text",
                )

            text = response.strip() if isinstance(response, str) else str(response).strip()
            if text:
                logger.info(f"STT: {len(audio_bytes)} bytes -> '{text[:100]}...'")
            return text

        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

    async def text_to_speech(self, text: str, max_length: int = 800) -> Optional[bytes]:
        """Синтезировать голосовое сообщение из текста."""
        if not text or not text.strip():
            return None

        clean_text = self._prepare_for_tts(text, max_length)
        if not clean_text:
            return None

        try:
            response = self.client.audio.speech.create(
                model=self.tts_model,
                voice=self.tts_voice,
                input=clean_text,
                response_format="opus",
            )
            audio_bytes = response.content
            logger.info(f"TTS: {len(clean_text)} chars -> {len(audio_bytes)} bytes")
            return audio_bytes

        except Exception as e:
            logger.error(f"TTS error: {e}")
            return None

    def _prepare_for_tts(self, text: str, max_length: int) -> str:
        """Подготовить текст для озвучки: убрать Markdown, эмодзи, обрезать."""
        # Убрать Markdown
        text = re.sub(r'[*_`#═─│┌┐└┘├┤┬┴┼]', '', text)
        text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)

        # Убрать эмодзи
        text = re.sub(
            r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF'
            r'\U0001F1E0-\U0001F1FF\U00002702-\U000027B0\U0001F900-\U0001F9FF'
            r'\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF\U00002600-\U000026FF'
            r'\U0000FE00-\U0000FE0F\U0000200D]+',
            '', text
        )

        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'  +', ' ', text)
        text = text.strip()

        if len(text) > max_length:
            cut = text[:max_length].rfind('.')
            if cut > max_length * 0.5:
                text = text[:cut + 1]
            else:
                cut = text[:max_length].rfind('\n')
                if cut > max_length * 0.3:
                    text = text[:cut]
                else:
                    text = text[:max_length] + "..."

        return text
