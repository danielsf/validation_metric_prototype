import getpass
import json
from subprocess import call
import numpy as np
import astropy.units as astropy_units
import lsst.verify as lsst_verify
import lsst.daf.persistence as daf_persistence
from JobContainerModule import JobContainer
from JobRunnerModule import JobRunner

class CtMetricCCD(JobContainer):

    def do_measurement(self, data_dict):
        data = data_dict[self.data_request[0]]
        src_ct = len(data['coord_ra'])
        ct_meas = lsst_verify.Measurement('dummy_ct_metric.SrcCt2',
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

        ct_meas = lsst_verify.Measurement('dummy_ct_metric.SrcCt2',
                                          src_ct*astropy_units.dimensionless_unscaled)

        ra = np.zeros(src_ct, dtype=float)
        dec = np.zeros(src_ct, dtype=float)

        i_start = 0
        for data_id in self.data_request:
            data = data_dict[data_id]
            local_ra = data['coord_ra']
            local_dec = data['coord_dec']
            ra[i_start:i_start+len(local_ra)] = local_ra
            dec[i_start:i_start+len(local_dec)] = local_dec
            i_start += len(local_ra)

        ct_meas.extras['ra_rad'] = lsst_verify.Datum(ra, label='ra_rad',
                                                     description='RA of sources in radians',
                                                     unit=astropy_units.radian)


        ct_meas.extras['dec_rad'] = lsst_verify.Datum(dec, label='dec_rad',
                                                     description='Dec of sources in radians',
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
    json_file_name = 'dummy_ct_metric_output.json'
    fp_metric.job.write(json_file_name)

    username = getpass.getuser()
    password = getpass.getpass(prompt='SQUASH password for {}:'.format(username))

    fp_metric.load_definitions_to_squash(username, password)

    squash_url = fp_metric._squash_api_url

    call(['dispatch_verify.py', '--ignore-lsstsw', '--url', '%s' % squash_url,
         '--user', '%s' % username, '--password', '%s' % password, '%s' % json_file_name])
