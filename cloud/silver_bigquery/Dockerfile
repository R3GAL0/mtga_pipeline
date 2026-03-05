
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

    
COPY . .

ENV TMP_BASE=/tmp/data
RUN mkdir -p $TMP_BASE


CMD ["python", "main.py"]