from MetricContainerModule import MetricContainer

class CtMetricContainer(MetricContainer):
    pass


if __name__ == "__main__":

    ct_metric = CtMetricContainer()
    ct_metric.metric_yaml = "metrics/ct_metric.yaml"
    ct_metric.specs_dir = "metrics/ct_metric"
