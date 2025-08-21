# Stage 1: Builder
# Install dependencies using uv
FROM python:3.13-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install uv
RUN pip install uv

# Copy requirements and install dependencies
WORKDIR /app
COPY requirements.txt .
RUN uv pip install --no-cache -r requirements.txt --system

# Stage 2: Final image
# Copy only the necessary files
FROM python:3.13-slim AS final

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy installed dependencies from the builder stage
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy the application code
COPY app ./app

# Copy the .env file
COPY .env .

# Expose port and run the application
EXPOSE 9999
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9999", "--loop", "uvloop", "--http", "httptools", "--workers", "1"]