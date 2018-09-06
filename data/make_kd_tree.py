import scipy.spatial as scipy_spatial
import h5py
import pickle
import numpy as np

with h5py.File('cosmos_data.h5', 'r') as input_file:
    ra = input_file['ra'].value
    dec = input_file['dec'].value
    pts = np.array([ra, dec]).transpose()
    tree = scipy_spatial.cKDTree(pts, leafsize=10, copy_data=True)
    pickle.dump(tree, open('cosmos_ra_dec_tree.p', 'wb'))
