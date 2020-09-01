import logging
from datetime import datetime, timedelta

from pyhocon import ConfigFactory  # noqa: F401
from pyhocon import ConfigTree  # noqa: F401
from typing import Dict, Iterable, Any  # noqa: F401

from databuilder import Scoped
from databuilder.task.base_task import Task  # noqa: F401
from databuilder.clients.neptune_client import NeptuneSessionClient
from databuilder.serializers.neptune_serializer import (
    NEPTUNE_CREATION_TYPE_NODE_PROPERTY_NAME,
    NEPTUNE_LAST_SEEN_AT_NODE_PROPERTY_NAME,
    NEPTUNE_CREATION_TYPE_EDGE_PROPERTY_NAME,
    NEPTUNE_LAST_SEEN_AT_EDGE_PROPERTY_NAME,
    NEPTUNE_CREATION_TYPE_JOB
)
from gremlin_python.process import traversal

LOGGER = logging.getLogger(__name__)


class NeptuneStalenessRemovalTask(Task):
    """
    A Specific task that is to remove stale nodes and relations in Neptune.
    It will use "published_tag" attribute assigned from Neo4jCsvPublisher and if "published_tag" is different from
    the one it is getting it from the config, it will regard the node/relation as stale.
    Not all resource is being published by Neo4jCsvPublisher and you can only set specific LABEL of the node or TYPE
    of relation to perform this deletion.
    """

    # Config keys
    NEPTUNE_HOST = 'neptune_host'
    AWS_ACCESS_KEY = 'aws_access_key'
    AWS_ACCESS_SECRET = 'aws_access_secret'
    AWS_REGION = 'aws_region'
    TARGET_NODES = "target_nodes"
    TARGET_RELATIONS = "target_relations"
    BATCH_SIZE = "batch_size"
    DRY_RUN = "dry_run"
    # Staleness max percentage. Safety net to prevent majority of data being deleted.
    STALENESS_MAX_PCT = "staleness_max_pct"
    # Staleness max percentage per LABEL/TYPE. Safety net to prevent majority of data being deleted.
    STALENESS_PCT_MAX_DICT = "staleness_max_pct_dict"
    # Sets how old the nodes and relationships can be
    STALENESS_CUT_OFF_IN_SECONDS = "staleness_cut_off_in_seconds"
    DEFAULT_CONFIG = ConfigFactory.from_dict({
        BATCH_SIZE: 100,
        STALENESS_MAX_PCT: 5,
        TARGET_NODES: [],
        TARGET_RELATIONS: [],
        STALENESS_PCT_MAX_DICT: {},
        DRY_RUN: False
    })

    def __init__(self):
        # type: () -> None
        pass

    def get_scope(self):
        # type: () -> str
        return 'task.remove_stale_data'

    def init(self, conf):
        # type: (ConfigTree) -> None
        conf = Scoped.get_scoped_conf(conf, self.get_scope()) \
            .with_fallback(conf) \
            .with_fallback(NeptuneStalenessRemovalTask.DEFAULT_CONFIG)
        self.target_nodes = set(conf.get_list(NeptuneStalenessRemovalTask.TARGET_NODES))
        self.target_relations = set(conf.get_list(NeptuneStalenessRemovalTask.TARGET_RELATIONS))
        self.batch_size = conf.get_int(NeptuneStalenessRemovalTask.BATCH_SIZE)
        self.dry_run = conf.get_bool(NeptuneStalenessRemovalTask.DRY_RUN)
        self.staleness_pct = conf.get_int(NeptuneStalenessRemovalTask.STALENESS_MAX_PCT)
        self.staleness_pct_dict = conf.get(NeptuneStalenessRemovalTask.STALENESS_PCT_MAX_DICT)
        self.neptune_host = conf.get_string(NeptuneStalenessRemovalTask.NEPTUNE_HOST)

        self.staleness_cut_off_in_seconds = conf.get_int(NeptuneStalenessRemovalTask.STALENESS_CUT_OFF_IN_SECONDS)
        self.cutoff_datetime = datetime.utcnow() - timedelta(seconds=self.staleness_cut_off_in_seconds)

        self._driver = NeptuneSessionClient(
            neptune_host=self.neptune_host,
            region=self.aws_region,
            access_secret=self.aws_secret_key,
            access_key=self.aws_access_key,
            session_token=self.aws_session_token
        )

    def run(self) -> None:
        """
        First, performs a safety check to make sure this operation would not delete more than a threshold where
        default threshold is 5%. Once it passes a safety check, it will first delete stale nodes, and then stale
        relations.
        :return:
        """
        self.validate()
        self._delete_stale_relations()
        self._delete_stale_nodes()

    def validate(self) -> None:
        """
        Validation method. Focused on limit the risk on deleting nodes and relations.
         - Check if deleted nodes will be within 10% of total nodes.
        """
        self._validate_node_staleness_pct()
        self._validate_relation_staleness_pct()

    def _delete_stale_nodes(self):
        filter_properties = [
            (NEPTUNE_CREATION_TYPE_NODE_PROPERTY_NAME, NEPTUNE_CREATION_TYPE_JOB, traversal.eq),
            (NEPTUNE_LAST_SEEN_AT_NODE_PROPERTY_NAME, self.cutoff_datetime, traversal.lt)
        ]
        self._driver.delete_nodes(
            filter_properties=filter_properties,
            node_labels=list(self.target_nodes)
        )

    def _delete_stale_relations(self):
        filter_properties = [
            (NEPTUNE_CREATION_TYPE_EDGE_PROPERTY_NAME, NEPTUNE_CREATION_TYPE_JOB, traversal.eq),
            (NEPTUNE_LAST_SEEN_AT_EDGE_PROPERTY_NAME, self.cutoff_datetime, traversal.lt)
        ]
        self._driver.delete_edges(
            filter_properties=filter_properties,
            edge_labels=list(self.target_relations)
        )

    def _validate_staleness_pct(self, total_records, stale_records, types):
        # type: (Iterable[Dict[str, Any]], Iterable[Dict[str, Any]], Iterable[str]) -> None

        total_count_dict = {record['type']: int(record['count']) for record in total_records}

        for record in stale_records:
            type_str = record['type']
            if type_str not in types:
                continue

            stale_count = record['count']
            if stale_count == 0:
                continue

            node_count = total_count_dict[type_str]
            stale_pct = stale_count * 100 / node_count

            threshold = self.staleness_pct_dict.get(type_str, self.staleness_pct)
            if stale_pct >= threshold:
                raise Exception('Staleness percentage of {} is {} %. Stopping due to over threshold {} %'
                                .format(type_str, stale_pct, threshold))

    def _validate_node_staleness_pct(self):
        # type: () -> None
        total_records = self._driver.get_all_nodes_grouped_by_label_filtered()
        filter_properties = [
            (NEPTUNE_CREATION_TYPE_NODE_PROPERTY_NAME, NEPTUNE_CREATION_TYPE_JOB, traversal.eq),
            (NEPTUNE_LAST_SEEN_AT_NODE_PROPERTY_NAME, self.cutoff_datetime, traversal.lt)
        ]
        stale_records = self._driver.get_all_nodes_grouped_by_label_filtered(
            filter_properties=filter_properties
        )
        self._validate_staleness_pct(
            total_records=total_records,
            stale_records=stale_records,
            types=self.target_nodes
        )

    def _validate_relation_staleness_pct(self):
        # type: () -> None
        total_records = self._driver.get_all_edges_grouped_by_label()
        filter_properties = [
            (NEPTUNE_CREATION_TYPE_EDGE_PROPERTY_NAME, NEPTUNE_CREATION_TYPE_JOB, traversal.eq),
            (NEPTUNE_LAST_SEEN_AT_EDGE_PROPERTY_NAME, self.cutoff_datetime, traversal.lt)
        ]
        stale_records = self._driver.get_all_edges_grouped_by_label(
            filter_properties=filter_properties
        )
        self._validate_staleness_pct(total_records=total_records,
                                     stale_records=stale_records,
                                     types=self.target_relations)
