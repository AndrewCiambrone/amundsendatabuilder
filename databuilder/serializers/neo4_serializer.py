from typing import Dict, Any
import six

from databuilder.models.graph_relationship import GraphRelationship
from databuilder.models.graph_node import GraphNode
from databuilder.models.neo4j_csv_serde import (
    NODE_LABEL,
    NODE_KEY,
    RELATION_END_KEY,
    RELATION_END_LABEL,
    RELATION_REVERSE_TYPE,
    RELATION_START_KEY,
    RELATION_START_LABEL,
    RELATION_TYPE
)

UNQUOTED_SUFFIX = ':UNQUOTED'


def convert_node(node):
    # type: (GraphNode) -> Dict[str, Any]
    node_dict = {
        NODE_LABEL: node.label,
        NODE_KEY: node.id
    }
    for key, value in node.node_attributes.items():
        neptune_value_type = _get_neo4j_suffix_value(value)
        doc_key = "{key}{suffix}".format(
            key=key,
            suffix=neptune_value_type
        )
        node_dict[doc_key] = value
    return node_dict


def convert_relationship(relationship):
    # type: (GraphRelationship) -> Dict[str, Any]
    base_relationship_doc = {
        RELATION_START_KEY: relationship.start_key,
        RELATION_START_LABEL: relationship.start_label,
        RELATION_END_KEY: relationship.end_key,
        RELATION_END_LABEL: relationship.end_label,
        RELATION_TYPE: relationship.type,
        RELATION_REVERSE_TYPE: relationship.reverse_type
    }

    for key, value in relationship.relationship_attributes.items():
        base_relationship_doc[key] = value

    return base_relationship_doc


def _get_neo4j_suffix_value(value):
    # type: (Any) -> str
    if isinstance(value, six.integer_types):
        return UNQUOTED_SUFFIX

    if isinstance(value, bool):
        return UNQUOTED_SUFFIX

    return ''
