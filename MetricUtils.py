import json

__all__ = ["are_data_requests_identical"]

def are_data_requests_identical(t1, t2):
    if t1[0] != t2[0]:
        return False

    d1 = json.dumps(t1[1], sort_keys=True)
    d2 = json.dumps(t2[1], sort_keys=True)
    if d1 != d2:
        return False

    return True

