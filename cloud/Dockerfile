
FROM python:3.11-slim

# Configure the environment
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# # Copy your ETL script
# # COPY ./main.py .
# COPY ./make_tables.py .
# COPY ./show_tables.py .
# # COPY ./Filtered_Player_20260212_233143_test.csv .
# # COPY ./Filtered_Player_20260212_221356_2games.csv .
# COPY ./mtga_local.duckdb .


# # Default command
# # CMD ["python", "main.py"]
# # CMD ["python", "make_tables.py"]
# # CMD ["python", "show_tables.py"]
# CMD python make_tables.py && python show_tables.py
# # STOP 


# Copy code in the directory
COPY . .

# Set environment variable for /tmp data
ENV TMP_BASE=/tmp/data
RUN mkdir -p $TMP_BASE

# Entry point for the job
CMD ["python", "main.py"]