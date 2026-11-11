if __name__ == "__main__":
    # Imported here so that the module can be loaded before the run module exists.
    from pypi_downloads import run

    raise SystemExit(run.main())
