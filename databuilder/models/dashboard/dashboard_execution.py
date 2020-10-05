import logging

from typing import Optional, Dict, Any, Union, Iterator  # noqa: F401

from databuilder.models.dashboard.dashboard_metadata import DashboardMetadata
from databuilder.models.graph_serializable import (GraphSerializable)

from databuilder.models.graph_node import GraphNode
from databuilder.models.graph_relationship import GraphRelationship

LOGGER = logging.getLogger(__name__)


class DashboardExecution(GraphSerializable):
    """
    A model that encapsulate Dashboard's execution timestamp in epoch and execution state
    """
    DASHBOARD_EXECUTION_LABEL = 'Execution'
    DASHBOARD_EXECUTION_KEY_FORMAT = '{product}_dashboard://{cluster}.{dashboard_group_id}/' \
                                     '{dashboard_id}/execution/{execution_id}'
    DASHBOARD_EXECUTION_RELATION_TYPE = 'EXECUTED'
    EXECUTION_DASHBOARD_RELATION_TYPE = 'EXECUTION_OF'

    LAST_EXECUTION_ID = '_last_execution'
    LAST_SUCCESSFUL_EXECUTION_ID = '_last_successful_execution'

    def __init__(self,
                 dashboard_group_id,  # type: Optional[str]
                 dashboard_id,  # type: Optional[str]
                 execution_timestamp,  # type: int
                 execution_state,  # type: str
                 execution_id=LAST_EXECUTION_ID,  # type: str
                 product='',  # type: Optional[str]
                 cluster='gold',  # type: str
                 **kwargs
                 ):
        self._dashboard_group_id = dashboard_group_id
        self._dashboard_id = dashboard_id
        self._execution_timestamp = execution_timestamp
        self._execution_state = execution_state
        self._execution_id = execution_id
        self._product = product
        self._cluster = cluster
        self._node_iterator = self._create_node_iterator()
        self._relation_iterator = self._create_relation_iterator()

    def create_next_node(self):
        # type: () -> Union[GraphNode, None]
        try:
            return next(self._node_iterator)
        except StopIteration:
            return None

    def _create_node_iterator(self):  # noqa: C901
        # type: () -> Iterator[GraphNode]
        node = GraphNode(
            key=self._get_last_execution_node_key(),
            label=DashboardExecution.DASHBOARD_EXECUTION_LABEL,
            attributes={
                'timestamp': self._execution_timestamp,
                'state': self._execution_state
            }
        )
        yield node

    def create_next_relation(self):
        # type: () -> Union[GraphRelationship, None]
        try:
            return next(self._relation_iterator)
        except StopIteration:
            return None

    def _create_relation_iterator(self):
        # type: () -> Iterator[GraphRelationship]
        relationship = GraphRelationship(
            start_label=DashboardMetadata.DASHBOARD_NODE_LABEL,
            start_key=DashboardMetadata.DASHBOARD_KEY_FORMAT.format(
                product=self._product,
                cluster=self._cluster,
                dashboard_group=self._dashboard_group_id,
                dashboard_name=self._dashboard_id
            ),
            end_label=DashboardExecution.DASHBOARD_EXECUTION_LABEL,
            end_key=self._get_last_execution_node_key(),
            type=DashboardExecution.DASHBOARD_EXECUTION_RELATION_TYPE,
            reverse_type=DashboardExecution.EXECUTION_DASHBOARD_RELATION_TYPE,
            attributes={}
        )
        yield relationship

    def _get_last_execution_node_key(self):
        return DashboardExecution.DASHBOARD_EXECUTION_KEY_FORMAT.format(
            product=self._product,
            cluster=self._cluster,
            dashboard_group_id=self._dashboard_group_id,
            dashboard_id=self._dashboard_id,
            execution_id=self._execution_id
        )

    def __repr__(self):
        return 'DashboardExecution({!r}, {!r}, {!r}, {!r}, {!r}, {!r}, {!r})'.format(
            self._dashboard_group_id,
            self._dashboard_id,
            self._execution_timestamp,
            self._execution_state,
            self._execution_id,
            self._product,
            self._cluster
        )
