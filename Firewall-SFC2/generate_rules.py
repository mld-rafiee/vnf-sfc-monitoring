import json
import random

protocols = ["tcp", "udp", "icmp"]
ip_prefixes = ["10.0.0.", "192.168.1."]
ports = range(1, 65536)  # All ports (adjust as needed)

# Generate a large number of rules (adjust num_rules for quantity)
num_rules = 10
rules = []
for _ in range(num_rules):
    rule = {
        "protocol": random.choice(protocols),
        "src_ip": random.choice(ip_prefixes) + str(random.randint(0, 255)),
        "src_port": random.choice(ports),
        "dst_ip": random.choice(ip_prefixes) + str(random.randint(0, 255)),
        "dst_port": random.choice(ports),
        "action": random.choice(["ALLOW", "DENY"])
    }
    rules.append(rule)

# Save the generated rules to a JSON file
with open('firewall_rules.json', 'w') as outfile:
    json.dump(rules, outfile, indent=4)

print(f"Generated firewall rules saved to firewall_rules.json")

