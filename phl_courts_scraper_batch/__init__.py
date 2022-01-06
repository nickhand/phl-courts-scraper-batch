from importlib.metadata import version
from pathlib import Path

# Set the version properly
__version__ = version(__package__)

# Data directory
HOME_DIR = (Path(__file__).parent / "..").resolve()
DATA_DIR = HOME_DIR / "data"

# Scraping sources
SOURCES = ["court_summary", "portal", "bail"]

# The app name on AWS
APP_NAME = __package__.replace("_", "-")
