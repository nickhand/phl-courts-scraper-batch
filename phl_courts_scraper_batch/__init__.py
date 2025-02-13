from importlib.metadata import version


# Set the version properly
__version__ = version(__package__)

# The command name
CMD = __package__.replace("_", "-")
