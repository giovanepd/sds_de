import json

def mock_api_call(_unused_param):
    """
    Mock of an API

    Args:
        _unused_param: placeholder.

    Returns:
        list: list of dicts as an API return.
    """
    response = [
    {
        "name": "Xabier",
        "age": 39,
        "office": ""
    },
    {
        "name": "Miguel",
        "office": "RIO"
    },
    {
        "name": "Fran",
        "age": 31,
        "office": "RIO"
    }
]
    return json.dumps(response)
