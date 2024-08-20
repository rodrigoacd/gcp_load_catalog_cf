### Introduction

This Python code is designed to update the catalog in Dataplex using a CSV file stored in Cloud Storage.

## How to Use Locally

For testing purposes outside of Cloud Storage, we use the `local.py` file.

1. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

2. Set the environment variable:
   ```
   $env:GOOGLE_APPLICATION_CREDENTIALS="service_account_key.json"
   ```

3. Fill in the variables in the `local.py` file:
   ```python
   project_id = "bch-prj-bdta-pocarq-dev-41f1"
   dataset_id = "demo_dataset"
   tag_template_id = "bch_gdd_universal_campo"
   location = "us-east4"

   bucketName = "stefanini_poc_datacatalog/bch-prj-bdta-pocarq-dev-41f1"
   fileName = "catalog-0.csv"
   ```

4. Upload the files to the bucket specified in `bucketName`.

5. Run the script:
   ```
   python local.py
   ```

## How to Deploy on Cloud Function

The `main.py` Python file is the one that should be deployed in the Cloud Function.

1. Go to create a Cloud Function:
   * Select 1st Gen
   * Choose the appropriate Region
   * Event Type: On (finalizing/creating) file in the selected bucket
   * Bucket: Select the desired bucket

2. Add the following variables:
   * `tag_template_id`
   * `location` (the location of the data)

If you attempt to apply the same tag twice, an error will appear in the logs on the second attempt.

Don't forget to set the environment variable:
```
$env:GOOGLE_APPLICATION_CREDENTIALS="service_account_key.json"
```