import json
import astropy.units as astropy_units
import lsst.verify as lsst_verify
import lsst.daf.persistence as daf_persistence
from MetricContainerModule import MetricContainer
from JobDriverModule import JobDriver

class CtMetricCCD(MetricContainer):

    def do_measurement(self, data_dict):
        data = data_dict[self.data_request[0]]
        src_ct = len(data['coord_ra'])
        ct_meas = lsst_verify.Measurement('dummy_ct_metric.SrcCts',
                                          src_ct*astropy_units.dimensionless_unscaled)

        data_dict = json.loads(self.data_request[0][1])
        i_ccd = data_dict['ccd']
        self._unique_id = 'CCD: %d' % i_ccd
        print('measured %d source count %d' % (i_ccd, src_ct))
        job = lsst_verify.Job.load_metrics_package()
        job.metrics.update(self._metric_set)
        job.specs.update(self._specs_set)
        job.measurements.insert(ct_meas)
        self._measurement = job

class CtMetricFP(MetricContainer):

    def do_measurement(self, data_dict):
        self._unique_id = 'Full Focal Plane'
        src_ct = 0
        for data_id in self.data_request:
            data = data_dict[data_id]
            src_ct += len(data['coord_ra'])

        ct_meas = lsst_verify.Measurement('dummy_ct_metric.SrcCts',
                                          src_ct*astropy_units.dimensionless_unscaled)
        print('measured fp source count %d' % src_ct)
        job = lsst_verify.Job.load_metrics_package()
        job.metrics.update(self._metric_set)
        job.specs.update(self._specs_set)
        job.measurements.insert(ct_meas)
        self._measurement = job


if __name__ == "__main__":

    butler = daf_persistence.Butler('/datasets/hsc/repo/rerun/DM-13666/WIDE')

    job_driver = JobDriver()
    job_driver.butler =butler

    fp_metric = CtMetricFP()
    fp_metric.metric_yaml = "metrics/dummy_ct_metric.yaml"
    fp_metric.specs_dir = "metrics/dummy_ct_metric"

    for i_ccd in range(29):
        data_id = {'filter':'HSC-Y', 'ccd':i_ccd, 'visit':374}
        if butler.datasetExists('src', dataId=data_id):
            ccd_metric = CtMetricCCD()
            ccd_metric.metric_yaml = "metrics/dummy_ct_metric.yaml"
            ccd_metric.specs_dir = "metrics/dummy_ct_metric"
            data_request = ('src', data_id)
            ccd_metric.add_data_request(data_request)
            fp_metric.add_data_request(data_request)
            job_driver.add_metric(ccd_metric)

    job_driver.add_metric(fp_metric)
    job_driver.run()
