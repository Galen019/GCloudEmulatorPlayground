import os
from google.cloud import pubsub_v1

# Set the environment variable to point to the emulator
os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"

# Initialize the Pub/Sub client
project_id = "my-project-id"
topic_id = "my-topic-id"
subscription_id = "my-subscription-id"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Create the subscription if it doesn't exist
try:
    subscriber.create_subscription(
        request={"name": subscription_path, "topic": topic_path}
    )
    print(f"Subscription {subscription_id} created.")
except Exception as e:
    print(f"Subscription {subscription_id} already exists or error: {e}")


# Define a callback to process messages
def callback(message):
    print(f"Received message: {message.data.decode('utf-8')}")
    message.ack()


# Subscribe to the subscription
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...")

# Keep the main thread alive to listen for messages for 30 seconds
timeout = 15.0  # seconds
try:
    streaming_pull_future.result(timeout=timeout)
except TimeoutError:
    print(f"{timeout} Timeout expired")
    streaming_pull_future.cancel()
    streaming_pull_future.result()  # Block until the shutdown is complete
