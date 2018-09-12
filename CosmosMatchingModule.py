import os
import h5py
import hashlib
import pickle

__all__ = ["CosmosMatcher"]


class CosmosMatcher(object):

    def __init__(self):
        data_dir = 'data'
        assert os.path.isdir(data_dir)
        tree_name = os.path.join(data_dir, 'cosmos_ra_dec_tree.p')
        tree_sum = hashlib.md5(open(tree_name, 'rb').read()).hexdigest()
        assert tree_sum == 'cc4e3940e11705334a0674e3f4d8fab3'
        with open(tree_name, 'rb') as tree_handle:
            self._tree = pickle.load(tree_handle)
        h5_name = os.path.join(data_dir, 'cosmos_data.h5')
        h5_sum = hashlib.md5(open(h5_name, 'rb').read()).hexdigest()
        assert h5_sum == 'c0435acb300c727b7b52ff3c7724da47'
        self._data = h5py.File(h5_name, 'r')


if __name__ == "__main__":

    matcher = CosmosMatcher()
