import random
import json
import time
from collections import deque
from flask import Flask, request, jsonify
import requests
import socket
import concurrent.futures
import collections
import threading
import datetime
import csv

from queue import Queue

# Lock for thread-safe deque operations
lock = threading.Lock()

packets_1 = collections.deque()


target_ip = "158.39.201.62"  # worker5

target_port = 32282 #worker1: 32282 worker2: 30278  worker3: 30214 worker4: 30903

#buffer = deque()
software_IoT_devices = 1000
sensor_rate = [4, 12, 25, 50]

buffer_size = 1000000  # Set the buffer size limit
data_batch_size = 4000  # Number of messages per batch

buffer = collections.deque()

max_packet_size = 1472  # Maximum size of each packet- MTU (Maximum Transmission Unit) 1500
#1500 MTU - 8 UDP header - 20 IPv4 header

duration_per_rate = 1  # seconds for each rate

with open('NortNetEdge.csv', 'r') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=';')
    # Emit the first and second row
    first_row = next(csvreader)
    headers = next(csvreader)
    speedtest_ul_mbps_index = headers.index("speedtest_ul_mbps")
    speedtest_ul_mbps_value = []
    for row in csvreader:
            speedtest_ul_mbps_value.append(float(row[speedtest_ul_mbps_index]))

start_time_transmission_rate = time.time()
i_transmission_rate = 0   
#with open("transmission_rate_index.txt", "r") as f:
           #i_transmission_rate = int(f.read().strip())


transmission_rate = speedtest_ul_mbps_value[i_transmission_rate]

def generate_sensor_message(sensor_id, name, type_, unit):
    # Generate a sensor measurement message
    current_time = datetime.datetime.now()

    # Format the time string without the date
    #time_string = current_time.strftime("%H:%M:%S.%f")
    message = {
        "Sensor ID": sensor_id,
        "Name": name,
        "Type": type_,
        "Unit": unit,
        "Value": round(random.uniform(-10, 40), 2),
        "Timestamp": time.time()#time_string #time.strftime(time_string) 
    }
    return json.dumps(message)



def generate_messages(sensor_id, name, type_, unit, duration):
    # Generate messages at a given rate for a certain duration
    msg_second = random.choice(sensor_rate)
    end_time = time.time() + duration
    interval = 1.0 / msg_second
    while time.time() < end_time:
        message = generate_sensor_message(sensor_id, name, type_, unit)
        # Add the message to the buffer
        if len(buffer) >= buffer_size:
            buffer.popleft()  # Remove the oldest message if buffer is full
        buffer.append(message)
        #print(len(buffer))
        time.sleep(interval)
    #print(len(buffer))
    #print(f"length of buffer: {len(buffer)}")

last_update_time = time.time()
#packets = []
def batch_and_send_1():
    # Take messages from the buffer based on data batch size
    global last_update_time
    last_update_time = time.time()
    while True:
        #print(len(buffer))
        while len(buffer) >= data_batch_size:
            #print(f"length of buffer: {len(buffer)}")
            l_buffer = len(buffer)
            batch = [buffer.popleft() for _ in range(min(data_batch_size, l_buffer))]
            batch_size = sum(len(msg) for msg in batch)
            
            # Split the batch into packets based on the maximum packet size
            current_packet = []
            current_size = 0

            for msg in batch:
                msgjhg = len(msg)
                msg_size = len(json.dumps(msg)) #len(msg)
                dfgfg = len(json.dumps(msg))
                #current_packet = current_packet = [msg]
                if current_size + msg_size > max_packet_size:
                    if current_packet:  # Only append non-empty packets
                        packets_1.append(json.dumps(current_packet))
                    current_packet = [msg]
                    current_size = msg_size
                else:
                    current_packet.append(msg)
                    current_size += msg_size

            if current_packet:
                packets_1.append(json.dumps(current_packet))

        global start_time_transmission_rate
        global transmission_rate
        global i_transmission_rate
        if packets_1:    
            if time.time() - start_time_transmission_rate > 60:
                i_transmission_rate = (i_transmission_rate + 1) % len(speedtest_ul_mbps_value)
                #with open("transmission_rate_index.txt", "w") as f:
                    #f.write(str(i_transmission_rate))
                transmission_rate = speedtest_ul_mbps_value[i_transmission_rate]
                start_time_transmission_rate = time.time()

            send_packets(transmission_rate,packets_1) #transmission_rate


def set_timestamp_packet(packet):
    pkt = json.loads(packet)  # Deserialize the entire packet once
    time_now = time.time()  # Get the current timestamp once

    # Update the "Timestamp" field for each item in the list
    for i in range(len(pkt)):
        item = json.loads(pkt[i])
        item["Timestamp"] = time_now  # Add or update the Timestamp field
        pkt[i] = json.dumps(item)  # Convert the updated dictionary back to a JSON string

    return json.dumps(pkt)




sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

def send_packets(traffic_rate_mbps, packets):
    thread_id = threading.get_ident()
    traffic_rate_mbps = traffic_rate_mbps #/ 10
    traffic_rate_bps = traffic_rate_mbps * 1000000  # Convert Mbps to bps
    traffic_rate_bytes_per_sec = traffic_rate_bps / 8  # Convert bps to Bps 

    total_bytes_sent_in_last_second = 0
    
    i = 0
    
    batch = []  # Initialize an empty batch

    while packets:

        for _ in range(20):
            if packets:
                packet = packets.popleft()
                packet = set_timestamp_packet(packet)
                batch.append(packet.encode())

        if len(batch) >= 1:
                # Join the batch into a single bytes object
                batch_data = b''.join(batch)
                batch_size_in_bytes = 0

                try:
                    sock.sendto(batch_data, (target_ip, target_port))

                except Exception as e:
                    print(f"Error sending batch to client: {e}")
                
                try:
                    # Send the entire batch at once
                    #with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s: #.SOCK_STREAM   .SOCK_DGRAM
                        #s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65535)
                        #s.connect((target_ip, target_port))
                        #s.sendall(batch_data)
                        sock.sendto(batch_data, (target_ip, target_port))
                        #print("First chunk sent")
                        #s.sendall(batch_data2)
                except Exception as e:
                    print(f"Error sending batch to client: {e}")


                # Clear the batch after sending
                # Calculate the size of the batch including the header overhead for each packet
                batch_size_in_bytes = sum(len(p) + 28 for p in batch) 
                total_bytes_sent_in_last_second += batch_size_in_bytes
                batch.clear()
                #batch2.clear()
                interval = batch_size_in_bytes / traffic_rate_bytes_per_sec
                #print("interval")
                time.sleep(interval-interval*0.8)#-0.011)
                #print("continue...")
                #time.sleep(0.01)
        

        
        global last_update_time
        current_time = time.time()
        #print(current_time - last_update_time)
        if current_time - last_update_time >= 1:
            #print(current_time - last_update_time)
            transmission_rate_bytes_per_sec = total_bytes_sent_in_last_second / (current_time - last_update_time)
            transmission_rate_mbps = transmission_rate_bytes_per_sec * 8 / (1024 * 1024)
                    #print(f"Transmission rate: {transmission_rate_mbps:.2f} Mbps")
            
            try:
                #requests.post("http://158.37.63.223:5006/kpi_transmission_rate", 
                              #json={"transmission_rate": transmission_rate_mbps})
                #print(f"Thread {thread_id}: Transmission rate: {transmission_rate_mbps:.2f} Mbps")
                print(transmission_rate_mbps)
                #print(len(buffer))
            except requests.RequestException as e:
                print(f"Failed to send response: {e}")

            total_bytes_sent_in_last_second = 0
            last_update_time = current_time


#@app.route('/start', methods=['POST'])
def start_generating_msg():

    sensor_info = [
        {"sensor_id": f"sensor_{i+1}", "name": f"Outdoor Temperature Sensor", "type_": "temperature", "unit": "Celsius"} 
        for i in range(software_IoT_devices)
    ]
    
    msg_second = 1  # 1 message per second per sensor
    duration = 1  # Duration for each sensor to run
    while True:
        with concurrent.futures.ThreadPoolExecutor(max_workers=software_IoT_devices) as executor:
            futures = [
                executor.submit(generate_messages, sensor["sensor_id"], sensor["name"], sensor["type_"], sensor["unit"], duration)
                for sensor in sensor_info
            ]

            concurrent.futures.wait(futures)


def start_threads(functions):
    threads = []
    for func in functions:
        thread = threading.Thread(target=func)
        thread.daemon = True
        thread.start()
        threads.append(thread)
    return threads
    

if __name__ == '__main__':

    thread = threading.Thread(target=batch_and_send_1)
    thread.daemon = True
    thread.start()
   
    start_generating_msg()

    