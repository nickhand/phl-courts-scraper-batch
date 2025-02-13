from pathlib import Path

import click

from .aws import AWS
from .scrape import scrape as _scrape


@click.group()
def cli():
    """The main entry point for the scraper."""
    pass


@cli.command(name="scrape")
@click.argument("flavor", type=click.Choice(["court_summary", "portal"]))
@click.argument("input_filename", type=str)
@click.argument("output_folder", type=str)
@click.option(
    "--search-by",
    type=click.Choice(["Incident Number", "Docket Number"]),
    help="How to search the portal",
)
@click.option(
    "--browser",
    type=click.Choice(["chrome", "firefox"]),
    help="The browser to use for scraping",
    default="chrome",
)
@click.option(
    "--nprocs",
    type=int,
    default=1,
    help="If running in parallel, the total number of processes that will run.",
)
@click.option(
    "--pid",
    type=int,
    default=0,
    help=(
        "If running in parallel, the local process id."
        "This should be between 0 and number of processes."
    ),
)
@click.option("--dry-run", is_flag=True, help="Do not save the results; dry run only.")
@click.option(
    "--sample",
    type=int,
    default=None,
    help="Only run a random sample of incident numbers.",
)
@click.option(
    "--log-freq",
    default=10,
    help="Log frequency within loop of scraping",
    type=int,
)
@click.option(
    "--seed",
    type=int,
    default=42,
    help="Random seed for sampling",
)
@click.option(
    "--errors",
    type=click.Choice(["ignore", "raise"]),
    default="ignore",
    help="Whether to ignore errors",
)
@click.option(
    "--sleep",
    default=2,
    help="Total waiting time b/w scraping calls (in seconds)",
    type=int,
)
@click.option(
    "--time-limit",
    default=20,
    help="Total waiting time to download a PDF (in seconds)",
    type=int,
)
@click.option(
    "--interval",
    default=1,
    help="How long to wait when downloading PDFs before checking for success",
    type=int,
)
@click.option("--aws", is_flag=True, help="Run scraping job on AWS")
@click.option(
    "--ntasks", default=20, type=int, help="The number of tasks to use on AWS."
)
@click.option("--no-wait", is_flag=True, help="Whether to wait for AWS jobs to finish")
@click.option("--debug", is_flag=True)
def scrape(
    flavor,
    input_filename,
    output_folder,
    search_by=None,
    browser="chrome",
    nprocs=None,
    pid=None,
    dry_run=False,
    sample=None,
    log_freq=10,
    seed=42,
    errors="ignore",
    sleep=2,
    interval=1,
    time_limit=20,
    aws=False,
    ntasks=20,
    no_wait=False,
    debug=False,
):
    """
    Scrape court-related data from the specified flavor of data.
    """

    # If we are on aws, paths need to be s3 buckets
    if aws:
        if not input_filename.startswith("s3://"):
            raise ValueError("Input filename must be an s3 bucket when running on AWS")
        if not output_folder.startswith("s3://"):
            raise ValueError("Output folder must be an s3 bucket when running on AWS")
    else:  # Running locally

        # Convert local paths to Path objects and resolve to absolute paths
        if not input_filename.startswith("s3://"):
            input_filename = str(Path(input_filename).resolve())
        if not output_folder.startswith("s3://"):
            output_folder = str(Path(output_folder).resolve())

    # "search_by" must be specified for flavor = "portal"
    if search_by is None and flavor == "portal":
        raise ValueError("'search_by' must be specified for flavor = 'portal'")

    # Get the arguments
    kwargs = {
        "flavor": flavor,
        "input_filename": input_filename,
        "output_folder": output_folder,
        "search_by": search_by,
        "pid": pid,
        "dry_run": dry_run,
        "sample": sample,
        "log_freq": log_freq,
        "seed": seed,
        "errors": errors,
        "sleep": sleep,
        "interval": interval,
        "time_limit": time_limit,
        "debug": debug,
        "browser": browser,
    }

    # Run job on AWS
    if aws:
        aws = AWS()
        return aws.submit_jobs(
            **kwargs,
            ntasks=ntasks,
            wait=(not no_wait),
        )
    # Run locally
    else:
        return _scrape(**kwargs, nprocs=nprocs)
