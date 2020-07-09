from typing import Any, Dict, List, Union  # noqa: F401

from databuilder.models.neo4j_csv_serde import Neo4jCsvSerializable
from databuilder.models.owner_constants import OWNER_RELATION_TYPE, OWNER_OF_OBJECT_RELATION_TYPE
from databuilder.models.user import User
from databuilder.models.graph_node import GraphNode
from databuilder.models.graph_relationship import GraphRelationship

class TableOwner(Neo4jCsvSerializable):
    # type: (...) -> None
    """
    Hive table owner model.
    """
    OWNER_TABLE_RELATION_TYPE = OWNER_OF_OBJECT_RELATION_TYPE
    TABLE_OWNER_RELATION_TYPE = OWNER_RELATION_TYPE

    def __init__(self,
                 db_name,  # type: str
                 schema,  # type: str
                 table_name,  # type: str
                 owners,  # type: Union[List, str]
                 cluster='gold',  # type: str
                 ):
        # type: (...) -> None
        self.db = db_name.lower()
        self.schema = schema.lower()
        self.table = table_name.lower()
        if isinstance(owners, str):
            owners = owners.split(',')
        self.owners = [owner.lower().strip() for owner in owners]

        self.cluster = cluster.lower()
        self._node_iter = iter(self.create_nodes())
        self._relation_iter = iter(self.create_relation())

    def create_next_node(self):
        # type: (...) -> Union[GraphNode, None]
        # return the string representation of the data
        try:
            return next(self._node_iter)
        except StopIteration:
            return None

    def create_next_relation(self):
        # type: (...) -> Union[GraphRelationship, None]
        try:
            return next(self._relation_iter)
        except StopIteration:
            return None

    def get_owner_model_key(self, owner  # type: str
                            ):
        # type: (...) -> str
        return User.USER_NODE_KEY_FORMAT.format(email=owner)

    def get_metadata_model_key(self):
        # type: (...) -> str
        return '{db}://{cluster}.{schema}/{table}'.format(db=self.db,
                                                          cluster=self.cluster,
                                                          schema=self.schema,
                                                          table=self.table)

    def create_nodes(self):
        # type: () -> List[GraphNode]
        """
        Create a list of Neo4j node records
        :return:
        """
        results = []
        for owner in self.owners:
            if owner:
                node = GraphNode(
                    id=self.get_owner_model_key(owner),
                    label=User.USER_NODE_LABEL,
                    node_attributes={
                        User.USER_NODE_EMAIL: owner
                    }
                )
                results.append(node)
        return results

    def create_relation(self):
        # type: () -> List[GraphRelationship]
        """
        Create a list of relation map between owner record with original hive table
        :return:
        """
        results = []
        for owner in self.owners:
            relationship = GraphRelationship(
                start_key=self.get_owner_model_key(owner),
                start_label=User.USER_NODE_LABEL,
                end_key=self.get_metadata_model_key(),
                end_label='Table',
                type=TableOwner.OWNER_TABLE_RELATION_TYPE,
                reverse_type=TableOwner.TABLE_OWNER_RELATION_TYPE
            )
            results.append(relationship)

        return results

    def __repr__(self):
        # type: () -> str
        return 'TableOwner({!r}, {!r}, {!r}, {!r}, {!r})'.format(self.db,
                                                                 self.cluster,
                                                                 self.schema,
                                                                 self.table,
                                                                 self.owners)
