# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
ADD . /app

# Install necessary dependencies
RUN apt-get update && apt-get install -y libpcap-dev

# Install any needed dependencies
RUN pip install --no-cache-dir -r requirements.txt
# Install netstat package
#RUN apt-get install -y net-tools

# Copy firewall_rules.json
COPY firewall_rules.json .

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Install Python dependencies
RUN pip install scapy

# Define environment variable
ENV PYTHONUNBUFFERED=1

# Run the Python script
CMD ["python", "firewall_server.py"]
