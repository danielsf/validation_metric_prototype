import json

from lsst.daf.persistence import Butler

from MetricContainerModule import MetricContainer

__all__ = ["JobDriver"]


class JobDriver(object):

    def __init__(self):
        self._butler = None
        self._metric_list = []
        self._data_dict = {}

    @property
    def butler(self):
        return self._butler

    @butler.setter
    def butler(self, val):
        if not isinstance(val, Butler):
            raise RuntimeError("Setting JobDriver.butler to "
                               "something that is not a Butler.\n"
                               "Actual class of input:\n\n"
                               "%s\n" % type(butler))

        self._butler = val

    @property
    def metric_list(self):
        return self._metric_list

    def add_metric(self, val):
        if not isinstance(val, MetricContainer):
            raise RuntimeError("Trying to put something that "
                               "is not a MetricContainer into "
                               "JobDriver._metric_list")

        self._metric_list.append(val)

    def _find_needed_data(self):

        id_to_ct = {}
        for metric in self._metric_list:
            if metric.measurement is None:
                continue

            for data in metric.data_request:
                if data in id_to_ct:
                    id_to_ct[data] += 1
                else:
                    id_to_ct[data] = 1

        return id_to_ct

    def _is_data_needed(self, data_id):
        for metirc in self._metric_list:
            if metric.measurement is None:
                continue
            if data_id in metric.data_request:
                return True

        return False

    def _run_all(self):
        for metric in self._metric_list:
            if metric.measurement is None:
                continue
            ready_to_run = True
            for data_id in metric.data_request:
                if data_id not in self._data_dict:
                    ready_to_run = False
                    break
                if ready_to_run:
                    metric.do_measurement(self._data_dict)

    def run(self):
        data_id_to_ct = self._find_needed_data()
        go_on = True
        while go_on:
            max_id = None
            max_ct = -1
            for data_id in data_id_to_ct:
                if self._is_data_needed(data_id):
                    if data_id_to_ct[data_id] > max_ct:
                        max_id = data_id
                        max_ct = data_id_to_ct[data_id]

            data = self._butler.get_data(data_id[0],
                                         dataId=json.loads(data_id[1]))
            self._data_dict[data_id] = data
            self._run_all()
            for data_id in self._data_dict:
                if not self._is_data_needed(data_id):
                    self._data_dict.pop(data_id)
