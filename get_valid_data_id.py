import lsst.daf.persistence as dafp
import json

butler = dafp.Butler('/datasets/hsc/repo/rerun/DM-13666/WIDE')

possible_id = butler.queryMetadata('src', ['visit', 'ccd', 'filter', 'field'])


ct = 0
with open('valid_hsc_id.txt', 'w') as out_file:
    for i_id, pid in enumerate(possible_id):
        data_id = {'visit':pid[0], 'ccd':pid[1], 'filter':pid[2], 'field':pid[3]}
        if butler.datasetExists('src', dataId=data_id):
            out_file.write('%s\n' % json.dumps(data_id))
            ct+=1

        if i_id%1000==0:
            print('did %d of %d' % (i_id, len(possible_id)))

print('all done')
