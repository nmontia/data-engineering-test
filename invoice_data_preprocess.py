import json

# Load the original JSON file
with open("resources/invoicing_data.json", "r") as file:
    data = json.load(file)

# Extract the invoices array
invoices = data["data"]["invoices"]

# Save the array to a new JSON file
with open("resources/invoicing_data_flat.json", "w") as outfile:
    json.dump(invoices, outfile, indent=4)