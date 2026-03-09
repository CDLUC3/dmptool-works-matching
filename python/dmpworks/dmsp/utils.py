import json


def serialise_json(data) -> str:
    """Serialize data to a JSON string.

    Args:
        data: The data to serialize.

    Returns:
        A JSON string representation of the data.
    """
    if isinstance(data, str):
        return data
    return json.dumps(data, sort_keys=True, separators=(",", ":"))
