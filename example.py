import astropy.units as astropy_units
import lsst.verify as lsst_verify
import lsst.daf.persistence as daf_persistence
from MetricContainerModule import MetricContainer
from JobDriverModule import JobDriver

class CtMetricContainer(MetricContainer):

    def do_measurement(self, data_dict):
        data = data_dict[self.data_request[0]]
        src_ct = len(data['coord_ra'])
        ct_meas = lsst_verify.Measurement('ct_metric.SrcCts',
                                          src_ct*astropy_units.dimensionless_unscaled)
        print('measured source count %d' % src_ct)
        job = lsst_verify.Job.load_metrics_package()
        job.metrics.update(self._metric_set)
        job.specs.update(self._specs_set)
        job.measurements.insert(ct_meas)
        self._measurement = job

if __name__ == "__main__":

    butler = daf_persistence.Butler('/datasets/hsc/repo/rerun/DM-13666/WIDE')

    ct_metric = CtMetricContainer()
    ct_metric.metric_yaml = "metrics/ct_metric.yaml"
    ct_metric.specs_dir = "metrics/ct_metric"
    data_request = ('src', {'filter':'HSC-Y', 'ccd':28, 'visit':374})
    ct_metric.add_data_request(data_request)

    job_driver = JobDriver()
    job_driver.butler =butler
    job_driver.add_metric(ct_metric)

    job_driver.run()
