import base64
import vertexai
from vertexai.generative_models import GenerativeModel, Part, FinishReason
import vertexai.preview.generative_models as generative_models
import pandas as pd
from google.cloud import bigquery
import json
import re
import functions_framework

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):

  generation_config = {
      "max_output_tokens": 8192,
      "temperature": 1,
      "top_p": 0.95,
  }

  safety_settings = {
      generative_models.HarmCategory.HARM_CATEGORY_HATE_SPEECH: generative_models.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
      generative_models.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: generative_models.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
      generative_models.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: generative_models.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
      generative_models.HarmCategory.HARM_CATEGORY_HARASSMENT: generative_models.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
  }

  data = cloud_event.data

  # event_id = cloud_event["id"]
  # event_type = cloud_event["type"]

  bucket = data["bucket"]
  name = data["name"]
  # metageneration = data["metageneration"]
  # timeCreated = data["timeCreated"]
  # updated = data["updated"]

  # print(f"Event ID: {event_id}")
  # print(f"Event type: {event_type}")
  print(f"Bucket: {bucket}")
  print(f"File: {name}")
  # print(f"Metageneration: {metageneration}")
  # print(f"Created: {timeCreated}")
  # print(f"Updated: {updated}")

  document1 = Part.from_uri(
  mime_type="application/pdf",
  uri="gs://" + bucket + "/" + name)

  vertexai.init(project="hackathonia", location="southamerica-east1")
  model = GenerativeModel(
    "gemini-1.5-flash-preview-0514",
    system_instruction=['''Extract from this invoice the data asked in this json as follows. The invoices are from either Whatsapp or Meta:{invoice_type: meta or whatsapp,invoice_number: the invoice number,invoice_date: invoice date,billing_period: billing period, payment_terms: payment terms,legal_name: the legal name of the customer the invoice is generated to,movements: a dictionary within a list containing the description or procuct as key and the amount as value. Search the description from page 2 onwards. Here is an example: "Argentina - Marketing List 11 -- $0.67", total_amount:float,currency:str} Date fields must be in YYYY-MM-DD format.''']
  )
  responses = model.generate_content(
      ["""Generate the json from this pdf according to the system instructions""", document1],
      generation_config=generation_config,
      safety_settings=safety_settings,
  )
  t = responses.candidates[0].content.parts[0].text
  t = t.replace("```", "").replace("\n", "")
  t = t[4:]
  t = json.loads(t)
  df = pd.DataFrame(t)

  df['file_name'] = name
  df['movements'] = df['movements'].astype(str)#.apply(lambda x: re.sub(r'{|}', '', x))
  
  # Construct a BigQuery client object.
  client = bigquery.Client()

  # Replace with your project ID and dataset name.
  project_id = "hackathonia"
  dataset_id = "invoices_dataset"
  table_id = "invoices"

  # Construct a full table reference.
  table_ref = client.dataset(dataset_id).table(table_id)

  # Replace `df` with your actual Colab output DataFrame.
  job_config = bigquery.LoadJobConfig(
      write_disposition="WRITE_APPEND",  # Replace with desired write disposition.
  )
  job = client.load_table_from_dataframe(
      df, table_ref, job_config=job_config
  )  # Pass job_config to control write behavior.
  job.result()  # Wait for the load job to complete.
  print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))




