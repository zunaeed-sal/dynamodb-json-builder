"""
DynamoDB JSON parser/builder script to be used for the following aws dynamodb batch-write-item call:
aws dynamodb batch-write-item --request-items file://<value> --region <value>

Run this command in CLI first before using:
aws dynamodb scan --table-name <value> --region <value> --output json > <value>.json

If you experience encoding issues in unix run this command first:
export LC_ALL=en_US.UTF-8
"""

# Import json library
import json

# Read in file, substitute path as necessary, load as dictionary
with open('/tmp/cdf_local_apm_table.json') as f:
    data = json.load(f) 

 
# Delete unnecessary items
del data['Count']
del data['ScannedCount']
del data['ConsumedCapacity']

# Rename tableName/key if desired
data['cdf-local-apm-registry'] = data.pop('Items')

# Extract list container from key
x = data['cdf-local-apm-registry']

# Instantiate builder dictionary
y = {"cdf-local-apm-registry":[]}

# Identify # of items to populate dictionary with
count = len(x)

# Iterator 
placeholder = 0

# Multiplier 
multiple = 1

# Begin building custom file writepath 
json_file_name = '/tmp/cdf_local_apm_export'

 
# Loop through with respect to # of items needed in order to populate dictionary; note: batch-write-item can only import up to 25 DynamoDB items per iteration, so intervals will be from [0,24]. Reset dictionary after each iteration.
for i in range(count):

    if (placeholder < (24*multiple)):

        y["cdf-local-apm-registry"].append({"PutRequest":{"Item":x[i]}})

        placeholder += 1

        if (placeholder == (24*multiple)):

            multiple += 1

            with open(json_file_name + str(multiple-1) + ".json", 'w') as f:

                json.dump(y, f)

                y = {"cdf-local-apm-registry":[]}


# Write out dictionary file of whatever items remain
with open(json_file_name + str(multiple) + ".json", 'w') as f:

    json.dump(y, f)


# Output builder dictionary in an aesthetically-pleasing manner, for testing purposes.
print(json.dumps(y, sort_keys=True, indent=4))
