from faker import Faker
import random
import csv

fake = Faker()

# Function to generate random data for a record
def generate_record():
    return {
        "CUST_KEY": fake.random_int(min=1, max=1000),
        "NAME": fake.name(),
        "ADDRESS": fake.address(),
        "NATION_KEY": fake.random_int(min=1, max=10),
        "PHONE": fake.phone_number(),
        "ACCOUNT_BALANCE": round(random.uniform(100, 5000), 2),
        "MARKET_SEGMENT": random.choice(["Segment1", "Segment2", "Segment3"]),
        "COMMENT": fake.text(max_nb_chars=117)
    }

# Generate 100 records
records = [generate_record() for _ in range(1000)]

# Write records to a CSV file
with open('my_customer_data.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=records[0].keys())
    writer.writeheader()
    writer.writerows(records)

print("CSV file with 100 records generated successfully.")
