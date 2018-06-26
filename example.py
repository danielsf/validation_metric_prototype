import getpass
import json
import astropy.units as astropy_units
import lsst.verify as lsst_verify
import lsst.daf.persistence as daf_persistence
from JobContainerModule import JobContainer
from JobRunnerModule import JobRunner

class CtMetricCCD(JobContainer):

    def do_measurement(self, data_dict):
        data = data_dict[self.data_request[0]]
        src_ct = len(data['coord_ra'])
        ct_meas = lsst_verify.Measurement('dummy_ct_metric.SrcCt',
                                          src_ct*astropy_units.dimensionless_unscaled)

        data_dict = json.loads(self.data_request[0][1])
        i_ccd = data_dict['ccd']
        self._unique_id = 'CCD: %d' % i_ccd
        print('measured %d source count %d' % (i_ccd, src_ct))
        job = lsst_verify.Job.load_metrics_package()
        job.metrics.update(self._metric_set)
        job.specs.update(self._specs_set)
        job.measurements.insert(ct_meas)
        self._job = job

class CtMetricFP(JobContainer):

    def do_measurement(self, data_dict):
        self._unique_id = 'Full Focal Plane'
        src_ct = 0
        for data_id in self.data_request:
            data = data_dict[data_id]
            src_ct += len(data['coord_ra'])

        ct_meas = lsst_verify.Measurement('dummy_ct_metric.SrcCt',
                                          src_ct*astropy_units.dimensionless_unscaled)

        subset_ra = data['coord_ra']
        ct_meas.extras['ra_sub'] = lsst_verify.Datum(subset_ra, label='ra_sub',
                                                     description='Subset of RA of sources',
                                                     unit=astropy_units.radian)

        print('measured fp source count %d' % src_ct)
        job = lsst_verify.Job.load_metrics_package()
        job.metrics.update(self._metric_set)
        job.specs.update(self._specs_set)
        job.measurements.insert(ct_meas)
        self._job = job


if __name__ == "__main__":

    butler = daf_persistence.Butler('/datasets/hsc/repo/rerun/DM-13666/WIDE')

    job_driver = JobRunner()
    job_driver.butler =butler

    fp_metric = CtMetricFP()
    fp_metric.metric_yaml = "metrics/dummy_ct_metric.yaml"
    fp_metric.specs_dir = "metrics/dummy_ct_metric"

    for i_ccd in range(29):
        data_id = {'filter':'HSC-Y', 'ccd':i_ccd, 'visit':374}
        if butler.datasetExists('src', dataId=data_id):
            data_request = ('src', data_id)
            fp_metric.add_data_request(data_request)

    job_driver.add_metric(fp_metric)
    job_driver.run()
    fp_metric.job.write('dummy_ct_metric_output.json')

    username = getpass.getuser()
    password = getpass.getpass(prompt='SQUASH password:')

    fp_metric.load_definitions_to_squash(username, password)
