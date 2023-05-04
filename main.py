from google.cloud import storage, vision
from google.cloud.vision import AsyncAnnotateFileRequest, InputConfig, Feature, GcsSource, GcsDestination, OutputConfig
import json
import re

filename = 'filename.pdf'
bucketname = 'bucketname'

# Set up the client
client = vision.ImageAnnotatorClient()

# Set the GCS URI
gcs_uri = f"gs://{bucketname}/{filename}"

# Create a GCS source
gcs_source = GcsSource(uri=gcs_uri)

# Create an input configuration
input_config = InputConfig(gcs_source=gcs_source, mime_type='application/pdf')

# Create a feature
feature = Feature(type_=Feature.Type.DOCUMENT_TEXT_DETECTION)

# Set the GCS destination
output_uri = f"gs://{bucketname}/{filename}/output.json"
gcs_destination = GcsDestination(uri=output_uri)

# Create an output configuration
output_config = OutputConfig(gcs_destination=gcs_destination, batch_size=1)

# Set up the request
request = AsyncAnnotateFileRequest(input_config=input_config, features=[feature], output_config=output_config)

# Send the request to the Cloud Vision API
operation = client.async_batch_annotate_files(requests=[request])

result = operation.result()

# Set up the storage client
storage_client = storage.Client()

# Get the bucket
bucket = storage_client.get_bucket(bucketname)

# Get the number of pages from the number of JSON files
blobs = bucket.list_blobs(prefix=f'{filename}/output.jsonoutput-')
pages_count = max(int(re.search(rf'{filename}/output.jsonoutput-(\d+)-to-(\d+).json', blob.name).group(1)) for blob in blobs)

# Extract the text
extracted_text = ''
for i in range(1, pages_count + 1):
    # Get the blob
    blob = bucket.blob(f'{filename}/output.jsonoutput-{i}-to-{i}.json')

    # Download the blob to a string
    json_string = blob.download_as_text()

    # Parse the JSON
    json_data = json.loads(json_string)

    for response in json_data['responses']:
        for page in response['fullTextAnnotation']['pages']:
            for block in page['blocks']:
                for paragraph in block['paragraphs']:
                    for word in paragraph['words']:
                        for symbol in word['symbols']:
                            extracted_text += symbol['text']
                        extracted_text += ' '
                    extracted_text += '\n'
                extracted_text += '\n'
            extracted_text += '\n'

# Create a new blob
blob = bucket.blob(f'{filename}/{filename}_output.txt')

# Upload the extracted text to GCS
blob.upload_from_string(extracted_text)
