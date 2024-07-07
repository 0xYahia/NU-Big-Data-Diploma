import csv
from confluent_kafka import Producer, KafkaError

# Initialize Kafka Producer
producer = Producer({
    'bootstrap.servers': 'kafka:29092',
    'client.id': 'my-producer'
})

# List to keep track of created topics
created_topics = []

# Read CSV and Create Topics
# with open('Car_Rates.csv', mode='r') as file:
#     reader = csv.DictReader(file)
#     brands = set(row['Brand'] for row in reader)

# # Process each brand and create topics if needed
# for brand in brands:
#     topic_name = f"{brand}_stream"
#     # Check if the topic already exists in the list
#     if topic_name not in created_topics:
#         try:
#             # Create the topic and add it to the list
#             producer.produce(topic_name, key=brand, value='')
#             producer.flush()
#             created_topics.append(topic_name)
#         except KafkaError as e:
#             if e.error_code() == KafkaError._PARTITION_EOF:
#                 print(f"Topic {topic_name} already exists.")
#             else:
#                 print(f"Failed to create topic {topic_name}: {e}")
#     else:
#         print(f"Topic {topic_name} already exists.")

# Reopen the file for message production
with open('Car_Rates.csv', mode='r') as file:
    reader = csv.DictReader(file)

    # Produce Messages
    for row in reader:
        topic_name = f"{row['Brand']}_stream"
        producer.produce(topic_name, key=row['Brand'].encode('utf-8'), value=str(row).encode('utf-8'))

        # Send the same row to a general topic
        general_topic_name = "general_stream"
        producer.produce(general_topic_name, key=row['General'].encode('utf-8'), value=str(row).encode('utf-8'))

# Wait for all messages to be delivered
producer.flush()

