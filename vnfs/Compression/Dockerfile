# Use the official Python base image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install necessary packages
RUN apt-get update && apt-get install -y \
    libpcap-dev \
    && pip install scapy brotli 

# Set the working directory
#WORKDIR /app

# Command to run the application
CMD ["python", "compression-vnf.py"]
