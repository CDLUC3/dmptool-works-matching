from importlib.resources import as_file, files


def get_fixtures_path():
    resource = files("tests.fixtures")

    if not resource.is_dir():
        raise FileNotFoundError(f"tests.fixtures path not found")

    with as_file(resource) as path:
        pass
    return path
