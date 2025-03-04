try:
    from importlib.metadata import version, PackageNotFoundError

    try:
        __version__ = version("batch-orchestra")
    except PackageNotFoundError:
        __version__ = "unknown"
except ImportError:
    # support python < 3.8; this code came from Claude
    try:
        import pkg_resources

        __version__ = pkg_resources.get_distribution("batch-orchestra").version
    except:
        __version__ = "unknown"
