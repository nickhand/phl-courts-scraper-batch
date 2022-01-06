import re
from pathlib import Path

from setuptools import find_packages, setup

PACKAGE_NAME = "gun_arrests_scraper"
HERE = Path(__file__).parent.absolute()


def get_requirements(filename):
    with filename.open("r") as fh:
        return [l.strip() for l in fh]


def find_version(*paths: str) -> str:
    with HERE.joinpath(*paths).open("tr") as fp:
        version_file = fp.read()
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    name=PACKAGE_NAME,
    version=find_version(PACKAGE_NAME, "__init__.py"),
    author="Nick Hand",
    maintainer="Nick Hand",
    maintainer_email="nick.hand@phila.gov",
    packages=find_packages(),
    description="Scraping court information for gun-related arrests in Philadelphia",
    license="MIT",
    python_requires=">=3.7",
    install_requires=get_requirements(HERE / "requirements.txt"),
    entry_points={
        "console_scripts": [
            "scrape_portal=gun_arrests_scraper.__main__:scrape_portal",
            "sync_aws_results=gun_arrests_scraper.__main__:sync_aws_results",
            "scrape_court_summaries=gun_arrests_scraper.__main__:scrape_court_summaries",
            "scrape_bail_info=gun_arrests_scraper.__main__:scrape_bail_info",
        ]
    },
    include_package_data=True,
)
