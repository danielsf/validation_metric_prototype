import os
import json

from MetricUtils import are_data_requests_identical

__all__ = ["MetricContainer"]

class MetricContainer(object):

    def __init__(self):
        self._metric_yaml = None
        self._specs_dir = None
        self._measurement = None
        self._data_request = []

    def do_measurement(self, data):
        raise NotImplementedError("Have not implemented do_measurement")

    @property
    def measurement(self):
        return self._measurement

    @property
    def metric_yaml(self):
        return self._metric_yaml

    @metric_yaml.setter
    def metric_yaml(self, val):
        if self._metric_yaml is not None:
            raise RuntimeError("Already set self._metric_yaml")

        if not os.path.exists(val) or not os.path.isfile(val):
            raise RuntimeError("%s is not a valid yaml file" % val)

        self._metric_yaml = val

    @property
    def specs_dir(self):
        retur self._specs_dir

    @specs_dir.setter
    def specs_dir(self, val)
        if self._specs_dir is not None:
            raise RuntimeError("Already set self._specs_dir")

        if not os.path.exists(val) or not os.path.isdir(val):
            raise RuntimeError("%s is not a valid directory for specs_dir")

    @property
    def data_request(self):
        return self._data_request

    def add_data_request(self, val):
        """
        val should be a tuple.  First element is the type of
        data set requested ('src', 'calexp', etc.).  Second
        element is the dataId.
        """
        if not isintance(val, tuple):
            raise RuntimeError("Must add tuples to data_request")

        if len(val) != 2:
            raise RuntimeError("Length of data_request tuple is %d" % len(val))

        if not isinstance(val[1], dict):
            raise RuntimeError("data_request[1] is not a dict")

        val_load = (val[0], json.dumps(val[1], sort_keys=True))

        is_duplicate = False
        for data in self._data_request:
            if are_data_requests_identical(data, val_load):
                is_duplicate = True
                break

        if not is_duplicate:
            self._data_request.append(val_load)
