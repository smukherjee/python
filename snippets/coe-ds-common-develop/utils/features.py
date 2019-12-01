"""
Feature Classes
"""

from utils.map import Map


class Metric(Map):
    """
    Feature Template
    """
    def __init__(self, logic=None, source=None, data_type=None, alias=None):
        super().__init__()
        self.logic = logic
        self.source = source
        self.data_type = data_type
        self.alias = alias

    def codify(self):
        select_string = f"'{self.alias}'"
        if self.logic != self.alias:
            select_string = f'({self.logic}).cast(\'{self.data_type}\').alias({select_string})'
        return select_string


class Metrics(Map):
    """
    List of metrics
    """
    def __init__(self, metrics_dict=None, data_type_dict=None, source_dict=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not metrics_dict:
            metrics_dict = Map()
        self.METRICS_DICT = metrics_dict
        if not data_type_dict:
            data_type_dict = Map()
        self.DATA_TYPE_DICT = data_type_dict
        if not source_dict:
            source_dict = self.Map()
        self.SOURCE_DICT = source_dict
        self.metrics = Map()
        self.populate()

    def populate(self, metrics_dict=None, data_type_dict=None):
        if not metrics_dict:
            metrics_dict = self.METRICS_DICT
        if not data_type_dict:
            data_type_dict = self.DATA_TYPE_DICT
        for col in metrics_dict.metrics:
            data_type = data_type_dict[col]
            logic = col
            if metrics_dict.logic:
                if col in list(metrics_dict.logic.keys()):
                    logic = metrics_dict.logic.__getattr__(col)
            source = metrics_dict.source.default  # Will implement exceptions later
            self.metrics.__setattr__(col, Metric(logic=logic, source=source, data_type=data_type, alias=col))

    def retrieve(self, evaluate=False):
        sources = list(set(metric.source for key, metric in self.metrics.items()))
        dfs = Map()
        for source in sources:
            dfs.__setattr__(source, f'spark.read.load(\'{self.SOURCE_DICT[source]}\')')
        if len(list(dfs.keys())) > 1:
            raise NotImplementedError("You need to implement a joiner function.")
        df = dfs[list(dfs.keys())[0]]
        fields = f'select({", ".join([metric.codify() for key, metric in self.metrics.items()])})'
        out = '.'.join([df, fields])
        if evaluate:
            return eval(out)
        return out


class MetricsFamily(Map):
    """
    Families of metrics
    """
    def __init__(self, metrics_dict=None, data_type_dict=None, source_dict=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not metrics_dict:
            metrics_dict = Map()
        self.METRICS_DICT = metrics_dict
        if not data_type_dict:
            data_type_dict = Map()
        self.DATA_TYPE_DICT = data_type_dict
        if not source_dict:
            source_dict = self.Map()
        self.SOURCE_DICT = source_dict
        self.metrics_family = Map()
        self.populate()

    def populate(self, metrics_dict=None, data_type_dict=None, source_dict=None):
        if not metrics_dict:
            metrics_dict = self.METRICS_DICT
        if not data_type_dict:
            data_type_dict = self.DATA_TYPE_DICT
        if not source_dict:
            source_dict = self.SOURCE_DICT
        for metrics_class in list(metrics_dict.keys()):
            class_metrics_dict = metrics_dict.__getattr__(metrics_class)
            metrics = Metrics(metrics_dict=class_metrics_dict, data_type_dict=data_type_dict, source_dict=source_dict)
            self.metrics_family.__setattr__(metrics_class, metrics)
