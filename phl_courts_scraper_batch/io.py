import tempfile
from pathlib import Path

import boto3
import pandas as pd
import simplejson as json
from dotenv import find_dotenv, load_dotenv


def upload_dataset_to_s3(data, bucket_name, s3_key):
    """
    Upload a dataset as a CSV file to a public AWS s3 bucket.

    Parameters
    ----------
    data : pd.DataFrame
        The dataset to upload.
    bucket_name : str
        The AWS bucket name
    s3_key : str
        The path on AWS within the bucket to save the dataset.
    """

    # Load the credentials
    load_dotenv(find_dotenv())

    # Initialize the s3 resource
    s3_client = boto3.client("s3")

    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=True) as tmpfile:
        # Save DataFrame to a compressed CSV file
        data.to_csv(tmpfile.name, index=False, compression="gzip")

        # Upload to s3
        s3_client.upload_file(
            tmpfile.name,
            bucket_name,
            s3_key,
            ExtraArgs={
                "ContentType": "application/csv",
                "ContentEncoding": "gzip",
                "ACL": "public-read",
            },
        )


def get_output_paths(flavor, output_folder, chunk):
    """Get the output paths."""

    if chunk is None:
        outfile = f"{flavor}_results.json"
    else:
        output_folder += "/chunks"
        outfile = f"{flavor}_results_{chunk}.json"

    outfile = f"{output_folder}/{outfile}"
    return output_folder, outfile


def load_input_data(flavor, input_filename, aws):
    """
    Load the input data for the scraper.

    If running locally, this will load the data from the local data directory.
    Otherwise, if running on AWS, it loads the data from s3.

    Parameters
    ----------
    flavor : str
    input_filename : str
    aws : AWS
    """
    # Make sure the infile exists
    if not aws.exists(input_filename):
        raise ValueError(f"Input filename '{input_filename}' does not exist.")

    # Determine where we are loading the data from
    if input_filename.startswith("s3://"):
        opener = aws.remote.open
    else:
        opener = aws.local.open

    # Load the data
    with opener(input_filename, "rb") as ff:

        # Load a CSV file
        if flavor == "portal":

            # Make sure it's a CSV file
            if not input_filename.endswith(".csv"):
                raise ValueError("Input file should end in .csv")

            # Return loaded data
            return pd.read_csv(
                ff, header=None, names=["value"], squeeze=True, dtype={"value": str}
            )
        else:  # A JSON file

            # Make sure it's a JSON file
            if not input_filename.endswith(".json"):
                raise ValueError("Input file should end in .json")

            # Return data
            return json.loads(ff.read())


def save_output_data(outfile, results, aws):
    """Save the output data for the scraper."""

    # Make sure local output folder exists
    if not outfile.startswith("s3://"):
        p = Path(outfile)
        if not p.parent.exists():
            p.parent.mkdir(parents=True)

    # Determine where we are loading the data from
    if outfile.startswith("s3://"):
        opener = aws.remote.open
    else:
        opener = aws.local.open

    # Write the data
    with opener(outfile, "w") as ff:

        if outfile.endswith(".json"):
            ff.write(json.dumps(results, ignore_nan=True))
        elif outfile.endswith(".csv"):
            results.to_csv(ff, index=False, header=False)
        else:
            raise ValueError("Input file should end in .json or .csv")
