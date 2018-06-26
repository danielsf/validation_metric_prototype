import json

from lsst.daf.persistence import Butler

from MetricContainerModule import MetricContainer

__all__ = ["JobRunner"]


class JobRunner(object):

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
            raise RuntimeError("Setting JobRunner.butler to "
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
                               "JobRunner._metric_list")

        self._metric_list.append(val)

    def _find_needed_data(self):

        id_to_ct = {}
        for metric in self._metric_list:
            if metric.measurement is not None:
                continue

            for data in metric.data_request:
                if data in id_to_ct:
                    id_to_ct[data] += 1
                else:
                    id_to_ct[data] = 1

        return id_to_ct

    def _is_data_needed(self, data_id):
        for metric in self._metric_list:
            if metric.measurement is not None:
                continue
            if data_id in metric.data_request:
                return True

        return False

    def _run_all(self):
        for metric in self._metric_list:
            if metric.measurement is not None:
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
        while len(data_id_to_ct)>0:
            max_id = None
            max_ct = -1
            for data_id in data_id_to_ct:
                if self._is_data_needed(data_id):
                    if data_id_to_ct[data_id] > max_ct:
                        max_id = data_id
                        max_ct = data_id_to_ct[data_id]

            print('loading ',max_id,len(data_id_to_ct))
            data = self._butler.get(max_id[0],
                                    dataId=json.loads(max_id[1]))

            data_id_to_ct.pop(max_id)

            self._data_dict[max_id] = data
            self._run_all()
            needs_popping = []
            for data_id in self._data_dict:
                if not self._is_data_needed(data_id):
                    needs_popping.append(data_id)

            for data_id in needs_popping:
                self._data_dict.pop(data_id)

        for metric in self._metric_list:
            if hasattr(metric, '_unique_id'):
                print('\n%s' % metric._unique_id)
            report = metric.measurement.report()
            report_table = report.make_table()
            print(report_table)
