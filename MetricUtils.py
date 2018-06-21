__all__ = ["are_data_requests_identical"]

def are_data_requests_identical(t1, t2):
    if t1[0] != t2[0]:
        return False

    k1 = list(t1[1].keys()).sort()
    k2 = list(t2[1].keys()).sort()

    if k1 != k2:
        return False

    for k in k1.keys():
        if k1[k] != k2[k]:
            return False

    return True

