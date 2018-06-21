import os

__all__ = ["MetricContainer"]

class MetricContainer(object):

    def __init__(self):
        self._metric_yaml = None
        self._specs_dir = None

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
