FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 DAGSTER_HOME=/app/.dagster
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt fastapi uvicorn
COPY . .
RUN mkdir -p /app/.dagster /app/data/state
EXPOSE 8000 3000
CMD ["uvicorn", "viz.server:app", "--host", "0.0.0.0", "--port", "8000"]
