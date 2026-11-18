
import json
import struct
import subprocess
import sys

# EDIT THIS CLASS PATH TO MATCH YOUR PROJECT.
JAVA_COMMAND = [
    "java",
    "-cp",
    r"target/classes;target/dependency/*",
    "Retrieval.RetrievalMain",
    "storage.properties",
]


def encode_request(request):
    body = json.dumps(request).encode("utf-8")
    return struct.pack(">I", len(body)) + body


def decode_response(data):
    if len(data) < 4:
        raise RuntimeError(
            f"Java returned incomplete response: {data!r}"
        )

    response_length = struct.unpack(">I", data[:4])[0]
    response_body = data[4:]

    if len(response_body) != response_length:
        raise RuntimeError(
            f"Expected {response_length} response bytes, "
            f"received {len(response_body)}"
        )

    return json.loads(response_body.decode("utf-8"))


def main():

    # Retrieve all agents over the entire timestamp range.
    request = {
        "source": "all",
        "start_time": 0,
        "end_time": 9223372036854775807,
        "event_type": None,
    }
    print("Sending retrieval request:")
    print(json.dumps(request, indent=2))

    framed_request = encode_request(request)

    process = subprocess.run(
        JAVA_COMMAND,
        input=framed_request,
        stdout=subprocess.PIPE,
        stderr=None,  # Java diagnostics remain visible
        timeout=60,
    )

    if process.returncode != 0:
        raise RuntimeError(
            f"Java process exited with code {process.returncode}"
        )

    response = decode_response(process.stdout)

    print("\n========== JAVA RESPONSE ==========")
    print(json.dumps(response, indent=2))

    if response.get("status") != "OK":
        print("\nRetrieval failed.")
        sys.exit(1)

    records = response.get("records", [])

    print(f"\nRecords retrieved: {len(records)}")

    print("\n========== RECORDS ==========")

    for record in records:
        print("----------------------------")
        print("Agent ID:", record.get("agentId"))
        print("Timestamp:", record.get("timestamp"))
        print("Event Type:", record.get("eventType"))
        print("Payload (Base64):", record.get("payload"))


if __name__ == "__main__":
    main()
