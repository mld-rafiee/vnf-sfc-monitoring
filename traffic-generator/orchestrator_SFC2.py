import subprocess
import threading
import time
import concurrent.futures
import requests


# Global variable to store the sum of all outputs
shared_sum = 0
# Lock to synchronize access to the shared variable
lock = threading.Lock()




def run_script(script_path, timeout):
    global shared_sum
    process = subprocess.Popen(
        ["python3", "-u", script_path], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE, 
        text=True,  # Ensure text output
        bufsize=1   # Line-buffered output
    )

    start_time = time.time()

    # Read stdout line by line and add it to the shared sum
    while True:
        try:
            # Read the next line of output
            #print("hi")
            output = process.stdout.readline().strip()
            #print(f"output: {output}")
            if output:
                # Simulate parsing and converting output to integer for summing (if applicable)
                try:
                    #print(f"Transmition rate: {output}", flush=True)
                    # Convert the output to an integer (assuming the script generates numbers)
                    output_value = float(output)
                    global shared_sum
                    with lock:
                        shared_sum += output_value  # Add to the shared sum
                        
                except ValueError:
                    print(f"Invalid output from {script_path}: {output}")
                    output_value = float(output)
                    with lock:
                        shared_sum += output_value

            # Check if process finished or timeout exceeded
            if process.poll() is not None or time.time() - start_time >= timeout:
                if process.poll() is None:
                    process.terminate()  # Force terminate if still running
                break

        except Exception as e:
            print(f"Exception in {script_path}: {e}")
            process.terminate()
            break

    process.terminate()

def print_shared_sum():
    while True:
        time.sleep(1)  # Print every second
        with lock:
            global shared_sum
            if shared_sum != 0:
                print(f"Transmition rate:: {shared_sum}", flush=True)
                try:
                    requests.post("http://158.37.63.223:5006/kpi_transmission_rate", 
                                json={"transmission_rate": shared_sum})
                except requests.RequestException as e:
                    print(f"Failed to send response: {e}")

                shared_sum = 0



def run_multiple_scripts(script_path, timeout, num_threads):
    # Use ThreadPoolExecutor to handle threads efficiently
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit multiple instances of the same script to run in parallel
        futures = [executor.submit(run_script, script_path, timeout) for _ in range(num_threads)]

        # Wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Will raise any exceptions encountered in the thread
            except Exception as e:
                print(f"Thread encountered an error: {e}")


timeout = 1800

# Create a thread to continuously print the shared sum every second
sum_thread = threading.Thread(target=print_shared_sum, daemon=True)

# Start the sum printing thread
sum_thread.start()

for i in range(96):
    print(f"Running {i}")
    for num_threads in [1, 10, 25, 50]:
        print(f"Running with {num_threads} threads")
        run_multiple_scripts("sender_SFC2.py", timeout, num_threads)
        
        # time.sleep(2)


