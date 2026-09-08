FROM python:3.11-slim-bookworm
WORKDIR /app
COPY requirements.txt requirements.lock ./
RUN pip install --no-cache-dir --only-binary=:all: -r requirements.txt -c requirements.lock
COPY app.py .
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080", "--no-access-log"]
