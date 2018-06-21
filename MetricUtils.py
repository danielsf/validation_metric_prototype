__all__ = ["are_data_requests_identical"]

def are_data_requests_identical(t1, t2):
    if t1[0] != t2[0]:
        return False

    if t1[1] != t2[1]:
        return False

    return True

