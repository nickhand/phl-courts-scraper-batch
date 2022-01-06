import click

from . import APP_NAME, DATA_DIR, SOURCES
from .aws import AWS
from .scrape import scrape as _scrape


@click.group()
def cli():
    """The main entry point for the scraper."""
    pass


@cli.command(name="scrape")
@click.argument("flavor", type=click.Choice(SOURCES))
@click.argument("dataset", type=str)
@click.option(
    "--search-by",
    type=click.Choice(["Incident Number", "Docket Number"]),
    help="How to search the portal",
)
@click.option("--tag", type=str, help="The tag to use for the dataset")
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
    help="Log frequency within loop of scraping PDFs",
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
@click.option("-o", "--output-folder", default=None, help="Output folder")
@click.option("--aws", is_flag=True, help="Run scraping job on AWS")
@click.option(
    "--ntasks", default=20, type=int, help="The number of tasks to use on AWS."
)
@click.option("--no-wait", is_flag=True, help="Whether to wait for AWS jobs to finish")
@click.option("--debug", is_flag=True)
def scrape(
    flavor,
    dataset,
    search_by=None,
    tag=None,
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
    output_folder=None,
    aws=False,
    ntasks=20,
    no_wait=False,
    debug=False,
):
    """Scrape court-related data from the specified source."""

    # Get the arguments
    kwargs = {
        "flavor": flavor,
        "dataset": dataset,
        "search_by": search_by,
        "tag": tag,
        "pid": pid,
        "dry_run": dry_run,
        "sample": sample,
        "log_freq": log_freq,
        "seed": seed,
        "errors": errors,
        "sleep": sleep,
        "interval": interval,
        "time_limit": time_limit,
        "output_folder": output_folder,
        "debug": debug,
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


@cli.command()
@click.option("--dry-run", is_flag=True, help="Do not save the results; dry run only.")
def sync_from_aws(from_aws=True, dry_run=False):
    """Sync scraping results from AWS."""
    aws = AWS()
    source = f"s3://{APP_NAME}"
    dest = DATA_DIR
    return aws.sync(source, dest, dry_run=dry_run)


@cli.command()
@click.option("--dry-run", is_flag=True, help="Do not save the results; dry run only.")
def sync_to_aws(dry_run=False):
    """Sync scraping results to/from AWS."""
    aws = AWS()
    dest = f"s3://{APP_NAME}"
    source = str(DATA_DIR)
    return aws.sync(source, dest, dry_run=dry_run)
