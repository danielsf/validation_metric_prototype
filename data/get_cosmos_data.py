import os
import h5py
import numpy as np
import gzip

data_dir = '/project/shared/data/COSMOS_catalogs'
assert os.path.isdir(data_dir)

date_tag = '20081101'

data_file_list = os.listdir(data_dir)
n_lines = 0
for data_file in data_file_list:
    if not data_file.endswith('.tbl.gz') or not (date_tag in data_file):
        continue

    data_name = os.path.join(data_dir, data_file)
    with gzip.open(data_name, 'rb') as in_file:
        for line in in_file:
            n_lines += 1
        n_lines -= 4

print('%d lines' % n_lines)

ra = np.zeros(n_lines, dtype=float)
dec = np.zeros(n_lines, dtype=float)
u = np.zeros(n_lines, dtype=float)
du = np.zeros(n_lines, dtype=float)
g = np.zeros(n_lines, dtype=float)
dg = np.zeros(n_lines, dtype=float)
r = np.zeros(n_lines, dtype=float)
dr = np.zeros(n_lines, dtype=float)
i = np.zeros(n_lines, dtype=float)
di = np.zeros(n_lines, dtype=float)
z = np.zeros(n_lines, dtype=float)
dz = np.zeros(n_lines, dtype=float)
star = np.zeros(n_lines, dtype=float)

i_line = -1
for data_file in data_file_list:
    if not data_file.endswith('.tbl.gz') or not (date_tag in data_file):
        continue

    data_name = os.path.join(data_dir, data_file)
    print('loading: ', data_name)
    with gzip.open(data_name, 'rb') as in_file:
        for dummy in range(4):
            in_file.readline()
        for line in in_file:
            i_line += 1
            line = line.decode('utf-8')
            line = line.strip()
            while '  ' in line:
                line = line.replace('  ',' ') 
            params = line.split(' ')
            try:
                id_val = int(params[0])
                ra[i_line] = float(params[3])
                dec[i_line] = float(params[4])
                star[i_line] = float(params[9])
                u[i_line] = float(params[31])
                du[i_line] = float(params[32])
                g[i_line] = float(params[33])
                dg[i_line] = float(params[34])
                r[i_line] = float(params[35])
                dr[i_line] = float(params[36])
                i[i_line] = float(params[37])
                di[i_line] = float(params[38])
                z[i_line] = float(params[39])
                dz[i_line] = float(params[40])
            except ValueError:
                print(line)
                print(params)
                raise

if i_line != n_lines-1:
    raise RuntimeError('i_line: %d ; should be %d' % (i_line, n_lines-1))

with h5py.File('cosmos_data.h5', 'w') as out_file:
    out_file.create_dataset('ra', data=ra, compression='gzip')
    out_file.create_dataset('dec', data=dec, compression='gzip')
    out_file.create_dataset('star', data=star, compression='gzip')
    out_file.create_dataset('u', data=u, compression='gzip')
    out_file.create_dataset('du', data=du, compression='gzip')
    out_file.create_dataset('g', data=g, compression='gzip')
    out_file.create_dataset('dg', data=dg, compression='gzip')
    out_file.create_dataset('r', data=r, compression='gzip')
    out_file.create_dataset('dr', data=dr, compression='gzip')
    out_file.create_dataset('i', data=i, compression='gzip')
    out_file.create_dataset('di', data=di, compression='gzip')
    out_file.create_dataset('z', data=z, compression='gzip')
    out_file.create_dataset('dz', data=dz, compression='gzip')

