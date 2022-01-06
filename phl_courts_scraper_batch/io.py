import os
from datetime import date
from pathlib import Path

import pandas as pd
import simplejson as json
from loguru import logger

from . import APP_NAME, DATA_DIR


def add_chunk_to_filename(path, chunk):
    """Add chunk to file path."""
    fields = os.path.splitext(path)
    return f"{fields[0]}_{chunk}{fields[1]}"


def get_output_paths(flavor, dataset, chunk, output_folder=None):
    """Get the output paths."""
    # Determine the output folder
    if output_folder is None:

        # Tag folder with today's date
        tag = date.today().strftime("%Y-%m-%d")
        output_folder = f"results/{dataset}/{tag}"

    if chunk is None:
        outfile = flavor + ".json"
    else:
        output_folder += "/chunks"
        outfile = add_chunk_to_filename(flavor, chunk) + ".json"

    outfile = f"{output_folder}/{outfile}"
    return output_folder, outfile


def load_input_data(
    flavor,
    dataset,
    aws,
    tag=None,
):
    """
    Load the input data for the scraper.

    If running locally, this will load the data from the local data directory.
    Otherwise, if running on AWS, it loads the data from s3.

    Parameters
    ----------
    flavor : str
    dataset : str
    aws : AWS
    tag : str, optional
    """
    # Get the input CSV
    if flavor == "portal":

        # The CSV path
        relpath = f"datasets/{dataset}.csv"

    # Get the input JSON
    else:
        path = f"results/{dataset}/"

        # Add the date tag
        if tag is None:
            tags = sorted(Path(path).glob("*"), reverse=True)
            if len(tags) > 0:
                tag = tags[0].name
            else:
                raise ValueError(
                    "No input data yet — do you need to run the portal scraper first?"
                )
        logger.info(f"Using latest tag: {tag}")
        path += tag

        # Add the filename
        relpath = f"{path}/{flavor}.json"

    # Get the prefix
    prefix = f"s3://{APP_NAME}" if aws.on_aws else str(DATA_DIR)

    # Create the infile
    infile = f"{prefix}/{relpath}"

    # Make sure the infile exists
    if not aws.exists(infile):
        raise ValueError(f"Infile '{infile}' does not exist.")

    # Determine where we are loading the data from
    if infile.startswith("s3://"):
        opener = aws.remote.open
    else:
        opener = aws.local.open

    # Load the data
    with opener(infile, "rb") as ff:

        # Load a CSV file
        if flavor == "portal":
            return pd.read_csv(
                ff, header=None, names=["value"], squeeze=True, dtype={"value": str}
            )
        else:  # A JSON file
            return json.loads(ff.read())


def save_output_data(outfile, results, aws):
    """Save the output data for the scraper."""

    # Get the prefix
    prefix = f"s3://{APP_NAME}" if aws.on_aws else str(DATA_DIR)

    # Create the infile
    outfile = f"{prefix}/{outfile}"

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
