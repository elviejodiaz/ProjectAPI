FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

 # Set environment variable for EULA acceptance before install
ENV ACCEPT_EULA=Y

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    ca-certificates \
    gcc \
    g++ \
 && rm -rf /var/lib/apt/lists/*

# Add Microsoft package repository securely
RUN curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg \
 && curl -sSL https://packages.microsoft.com/config/debian/11/prod.list -o /etc/apt/sources.list.d/mssql-release.list

# Clean up conflicting ODBC libraries first, then install only Microsoft's required ones
RUN apt-get update \
 && apt-get remove -y libodbc2 libodbcinst2 unixodbc-common || true \
 && apt-get install -y --allow-downgrades msodbcsql17 \
 && rm -rf /var/lib/apt/lists/*

 # Set working directory
WORKDIR /API

# Copy dependencies
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY ./API /API

# Expose the port
EXPOSE 8000

# Run the FastAPI app using uvicorn
CMD ["uvicorn", "loaddata:app", "--host", "0.0.0.0", "--port", "8000"]
