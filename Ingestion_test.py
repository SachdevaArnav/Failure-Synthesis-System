
import struct
import threading
import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883
TOPIC = "fleet/telemetry"

AGENT_COUNT = 100
MESSAGES_PER_AGENT = 20

def publish_agent(agent_id):
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"test-agent-{agent_id}"
    )

    client.connect(BROKER, PORT)
    client.loop_start()

    try:
        for sequence in range(MESSAGES_PER_AGENT):
            payload = struct.pack(">I", sequence)

            message = struct.pack(
                ">IQII",
                agent_id,
                100000 + sequence,
                len(payload),
                2
            ) + payload

            info = client.publish(TOPIC, message, qos=0)
            info.wait_for_publish()

        print(f"Agent {agent_id}: published {MESSAGES_PER_AGENT} messages")

    finally:
        client.disconnect()
        client.loop_stop()


threads = []

for agent_id in range(1, AGENT_COUNT + 1):
    thread = threading.Thread(
        target=publish_agent,
        args=(agent_id,)
    )
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

print("Concurrent publishing completed.")
