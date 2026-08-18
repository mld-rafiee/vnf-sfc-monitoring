# Switch to Ubuntu 22.04 — has snort in apt repos
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install Python + all system deps in one layer
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    libpcap-dev \
    tcpdump \
    iproute2 \
    net-tools \
    snort \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

RUN pip3 install --no-cache-dir -r requirements.txt

# Copy snort config into the correct location
RUN mkdir -p /etc/snort/rules
COPY snort.conf /etc/snort/snort.conf

CMD ["python3", "snort_dpi_enhanced.py"]