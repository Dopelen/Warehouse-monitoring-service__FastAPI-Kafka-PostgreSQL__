FROM python:3.11-slim

WORKDIR /app

# Копируем метаданные проекта
COPY pyproject.toml poetry.lock* ./

# Устанавливаем Poetry и зависимости
RUN pip install --no-cache-dir poetry \
    && poetry install --no-root

# Копируем приложение и миграции
COPY app/ ./app
COPY alembic/ ./alembic/
COPY alembic.ini .
COPY wait_for_db.py .

# # Команда запуска: ждём БД, применяем миграции и стартуем FastAPI
CMD ["sh", "-c", "poetry run python wait_for_db.py && poetry run alembic upgrade head && poetry run uvicorn app.main:app --host 0.0.0.0 --port 8000"]