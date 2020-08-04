import logging
from typing import Tuple, Callable, List, Dict
from collections import namedtuple
from itertools import chain
from pyhocon import ConfigTree  # noqa: F401

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor  # noqa: F401
from databuilder.loader.base_loader import Loader  # noqa: F401
from databuilder.task.base_task import Task  # noqa: F401
from databuilder.transformer.base_transformer import Transformer  # noqa: F401
from databuilder.transformer.base_transformer \
    import NoopTransformer  # noqa: F401
from databuilder.utils.closer import Closer


LOGGER = logging.getLogger(__name__)


class ChildMergeTaskExtractorWrapper:
    def __init__(self,
                 extractor,
                 key_mapper,
                 property_name_to_getter_mappers):
        # type: (Extractor, Callable, Dict[str, Callable]) -> None
        self.extractor = extractor
        self.key_mapper = key_mapper
        self.property_name_to_getter_mappers = property_name_to_getter_mappers
        self.results = {}
        self.is_initialized = False

    def init(self, conf):
        # type: (ConfigTree) -> None
        self.extractor.init(conf)
        self.is_initialized = True

    def run_extraction(self):
        assert self.is_initialized
        record = self.extractor.extract()
        while record:
            record_key = self.key_mapper(record)
            record_value = {}
            for property_name, property_getter in self.property_name_to_getter_mappers.items():
                record_value[property_name] = property_getter(record)

            self.results[record_key] = record_value
            record = self.extractor.extract()


class MergeTask(Task):
    """
    A merge task is like the default task except that it takes in 1 or more extractors

    Merges them based on there key and then transforms and loads

    If 2 of the children extractors map to the same property name the value is picked at random.

    The Parent extractor has final say of what the value will be.

    """

    # Determines the frequency of the log on task progress
    PROGRESS_REPORT_FREQUENCY = 'progress_report_frequency'

    def __init__(self,
                 parent_extractor,
                 parent_record_identifiers,
                 parent_record_property_name_getters,
                 parent_record_property_name_setters,
                 child_extractor_wrappers,
                 loader,
                 transformer=NoopTransformer()):
        # type: (Extractor, Callable, Dict[str, Callable], Dict[str, Callable], List[ChildMergeTaskExtractorWrapper], Loader, Transformer) -> None
        self.parent_extractor = parent_extractor
        self.parent_record_identifiers = parent_record_identifiers
        self.parent_record_property_name_getters = parent_record_property_name_getters
        self.parent_record_property_name_setters = parent_record_property_name_setters
        self.child_extractor_wrappers = child_extractor_wrappers
        self.transformer = transformer
        self.loader = loader

        self._closer = Closer()
        self._closer.register(self.parent_extractor.close)
        for child_extractor_wrapper in self.child_extractor_wrappers:
            child_extractor = child_extractor_wrapper.extractor
            self._closer.register(child_extractor.close)
        self._closer.register(self.transformer.close)
        self._closer.register(self.loader.close)

    def init(self, conf):
        # type: (ConfigTree) -> None
        self._progress_report_frequency = \
            conf.get_int('{}.{}'.format(self.get_scope(), MergeTask.PROGRESS_REPORT_FREQUENCY), 500)

        self.parent_extractor.init(Scoped.get_scoped_conf(
            conf,
            self.parent_extractor.get_scope())
        )
        for child_extractor_wrapper in self.child_extractor_wrappers:
            child_extractor = child_extractor_wrapper.extractor
            child_extractor_wrapper.init(Scoped.get_scoped_conf(conf, child_extractor.get_scope()))
        self.transformer.init(Scoped.get_scoped_conf(conf, self.transformer.get_scope()))
        self.loader.init(Scoped.get_scoped_conf(conf, self.loader.get_scope()))

    def run(self):
        # type: () -> None
        """
        Runs a task
        :return:
        """
        LOGGER.info('Running a task')
        self._run_child_extractors()
        try:
            record = self.parent_extractor.extract()
            count = 1
            while record:
                record = self.transformer.transform(record)
                if not record:
                    record = self.parent_extractor.extract()
                    continue
                self.loader.load(record)
                record = self.parent_extractor.extract()
                count += 1
                if count > 0 and count % self._progress_report_frequency == 0:
                    LOGGER.info('Extracted {} records so far'.format(count))

        finally:
            self._closer.close()

    def merge_record_with_children(self, record):
        record_keys = self.parent_record_identifiers(record)
        child_extractor_value_list = list(chain(*[
            [
                (record_id, child_record) for record_id, child_record in child_extractor.results.items()
                if record_id in record_keys
            ]
            for child_extractor in self.child_extractor_wrappers
            if set(record_keys) & set(child_extractor.results.keys())
        ]))
        child_extractor_values = {}
        for (record_id, child_value) in child_extractor_value_list:
            child_extractor_values.update(child_extractor_value)

        for property_name, property_value in child_extractor_values.items():
            record_setter = self.parent_record_property_name_setters.get(property_name)
            record_getter = self.parent_record_property_name_getters.get(property_name)
            if record_setter is None:
                LOGGER.info("Extractored {} but a setter was not found parent mapper. SKIPPING".format(property_name))
                continue

            if record_getter is None:
                LOGGER.info("Extractored {} but a getter was not found parent mapper. SKIPPING".format(property_name))
                continue

            if record_getter(record) is not None:
                LOGGER.info("Extractored {} but found it already set on parent. SKIPPING".format(property_name))
                continue

            record_setter(record, property_value)

        return record

    def _run_child_extractors(self):
        for child_extractor_wrapper in self.child_extractor_wrappers:
            child_extractor_wrapper.run_extraction()
