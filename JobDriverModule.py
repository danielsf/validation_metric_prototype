from lsst.daf.persistence import Butler

__all__ = ["JobDriver"]

class JobDriver(object):

    def __init__(self):
        self._butler = None
        self._metric_list = None

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
