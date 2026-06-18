FROM python:3.12-slim

# Install system dependencies: exiftool and curl (for healthcheck).
# libarchive-zip-perl gives exiftool's Archive::Zip so it can read INSIDE ZIP-based
# Office formats (docx/xlsx/pptx) and report their real type — without it they show as ZIP.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libimage-exiftool-perl \
    libarchive-zip-perl \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data and log directories
RUN mkdir -p /app/data/retained_files /app/data/temp /app/logs

# Default command: API server (overridden per service in docker-compose.yml)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
