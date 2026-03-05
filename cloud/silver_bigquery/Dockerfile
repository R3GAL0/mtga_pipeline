
FROM python:3.11-slim

# Configure the environment
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .
# COPY ./main.py .
# COPY ./make_tables_bigquery.py .
# # COPY ./show_tables.py .
# COPY ./requirements.txt .

# Set environment variable for /tmp data
ENV TMP_BASE=/tmp/data
RUN mkdir -p $TMP_BASE

# Entry point for the job
# CMD python main.py
CMD ["python", "main.py"]