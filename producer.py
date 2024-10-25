import os
from google.cloud import pubsub_v1

# Set the environment variable to point to the emulator
os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"

# Initialize the Pub/Sub client
project_id = "my-project-id"
topic_id = "my-topic-id"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Create the topic if it doesn't exist
try:
    publisher.create_topic(request={"name": topic_path})
    print(f"Topic {topic_id} created.")
except Exception as e:
    print(f"Topic {topic_id} already exists or error: {e}")

# Publish a message
for i in range(10):
    message = f"Message number {i}"
    future = publisher.publish(topic_path, message.encode("utf-8"))
    print(f"Published message ID: {future.result()}")
