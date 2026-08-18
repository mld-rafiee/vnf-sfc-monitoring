# Dockerfile

FROM python:3.9-slim

WORKDIR /app


# Copy the requirements file
#COPY requirements.txt requirements.txt
# Copy the current directory contents into the container at /app
COPY . /app

RUN apt-get update && apt-get install -y \
    libpcap-dev

COPY requirements.txt .
# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt



# Copy the service code
#COPY encryption_service.py .
RUN pip install scapy

# Expose the port the service will run on
EXPOSE 8080

# Run the service
CMD ["python", "encryption_service.py"]
