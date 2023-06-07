import json
import os

# Define path to directory containing JSON files
directory = "/Users/jesper/Projects/MTX/snowflake_app/update_information/updated_gpt"

# Create an empty list to hold the JSON data
json_data = []

# Loop through each file in the directory and load the JSON data
for filename in os.listdir(directory):
    if filename.endswith(".json"):
        with open(os.path.join(directory, filename), "r") as f:
            data = json.load(f)
            json_data.append(data)

# Combine the JSON data in the list into a single JSON object with the field name "nopho_table"
combined_data = {"nopho_table": json_data}

# Save the combined JSON object to a file
with open("combined_data.json", "w") as f:
    json.dump(combined_data, f, indent=4)
