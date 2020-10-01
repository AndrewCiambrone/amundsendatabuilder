import copy
from six import string_types

from typing import Iterable, Union, Iterator, Dict, Set, Tuple  # noqa: F401

from databuilder.models.cluster import cluster_constants
from databuilder.models.neo4j_csv_serde import Neo4jCsvSerializable

from databuilder.models.schema import schema_constant

from databuilder.models.graph_node import GraphNode
from databuilder.models.graph_relationship import GraphRelationship

DESCRIPTION_NODE_LABEL_VAL = 'Description'
DESCRIPTION_NODE_LABEL = DESCRIPTION_NODE_LABEL_VAL


class TagMetadata(Neo4jCsvSerializable):
    TAG_NODE_LABEL = 'Tag'
    TAG_KEY_FORMAT = '{tag}'
    TAG_TYPE = 'tag_type'
    DEFAULT_TYPE = 'default'
    BADGE_TYPE = 'badge'
    DASHBOARD_TYPE = 'dashboard'
    METRIC_TYPE = 'metric'

    def __init__(self,
                 name,  # type: str,
                 tag_type='default',  # type: str
                 ):
        self._name = name
        self._tag_type = tag_type
        self._nodes = iter([self.create_tag_node(self._name, self._tag_type)])
        self._relations = iter([])

    @staticmethod
    def get_tag_key(name):
        # type: (str) -> str
        if not name:
            return ''
        return TagMetadata.TAG_KEY_FORMAT.format(tag=name)

    @staticmethod
    def create_tag_node(name, tag_type=DEFAULT_TYPE):
        node = GraphNode(
            id=TagMetadata.get_tag_key(name),
            label=TagMetadata.TAG_NODE_LABEL,
            node_attributes={
                TagMetadata.TAG_TYPE: tag_type
            }
        )
        return node

    def create_next_node(self):
        # type: (...) -> Union[GraphNode, None]
        # return the string representation of the data
        try:
            return next(self._nodes)
        except StopIteration:
            return None

    def create_next_relation(self):
        # type: () -> Union[GraphRelationship, None]
        # We don't emit any relations for Tag ingestion
        try:
            return next(self._relations)
        except StopIteration:
            return None


class DescriptionMetadata:
    DESCRIPTION_NODE_LABEL = DESCRIPTION_NODE_LABEL_VAL
    PROGRAMMATIC_DESCRIPTION_NODE_LABEL = 'Programmatic_Description'
    DESCRIPTION_KEY_FORMAT = '{description}'
    DESCRIPTION_TEXT = 'description'
    DESCRIPTION_SOURCE = 'description_source'

    DESCRIPTION_RELATION_TYPE = 'DESCRIPTION'
    INVERSE_DESCRIPTION_RELATION_TYPE = 'DESCRIPTION_OF'

    # The default editable source.
    DEFAULT_SOURCE = "description"

    def __init__(self,
                 text,  # type: Union[None, str]
                 source=DEFAULT_SOURCE,  # type: str
                 description_owner_key=None, #type: Union[None, str]
                 ):
        """
        :param source: The unique source of what is populating this description.
        :param text: the description text. Markdown supported.
        """
        self._source = source
        self._text = text
        self.description_owner_key = description_owner_key
        #  There are so many dependencies on Description node, that it is probably easier to just separate the rest out.
        if (self._source == self.DEFAULT_SOURCE):
            self._label = self.DESCRIPTION_NODE_LABEL
        else:
            self._label = self.PROGRAMMATIC_DESCRIPTION_NODE_LABEL

    @staticmethod
    def create_description_metadata(text, source=DEFAULT_SOURCE):
        # type: (Union[None, str], str) -> DescriptionMetadata

        # We do not want to create a node if there is no description text!
        if text is None:
            return None
        if not source:
            description_node = DescriptionMetadata(text=text, source=DescriptionMetadata.DEFAULT_SOURCE)
        else:
            description_node = DescriptionMetadata(text=text, source=source)
        return description_node

    def get_description_id(self):
        # type: () -> str
        if self._source == self.DEFAULT_SOURCE:
            return "_description"
        else:
            return "_" + self._source + "_description"

    def __repr__(self):
        # type: () -> str
        return 'DescriptionMetadata({!r}, {!r})'.format(self._source, self._text)

    def get_node(self, node_key):
        # type: (str) -> GraphNode
        node = GraphNode(
            id=node_key,
            label=self._label,
            node_attributes={
                DescriptionMetadata.DESCRIPTION_SOURCE: self._source,
                DescriptionMetadata.DESCRIPTION_TEXT: self._text
            }
        )
        return node

    def get_relation(self, start_node, start_key, end_key):
        # type: (str, str, str) -> GraphRelationship
        relationship = GraphRelationship(
            start_label=start_node,
            start_key=start_key,
            end_label=self._label,
            end_key=end_key,
            type=DescriptionMetadata.DESCRIPTION_RELATION_TYPE,
            reverse_type=DescriptionMetadata.INVERSE_DESCRIPTION_RELATION_TYPE,
            relationship_attributes={}
        )
        return relationship


class ColumnMetadata:
    COLUMN_NODE_LABEL = 'Column'
    COLUMN_KEY_FORMAT = '{db}://{cluster}.{schema}/{tbl}/{col}'
    COLUMN_NAME = 'name'
    COLUMN_TYPE = 'type'
    COLUMN_ORDER = 'sort_order'  # int value needs to be unquoted when publish to neo4j
    COLUMN_DESCRIPTION = 'description'
    COLUMN_DESCRIPTION_FORMAT = '{db}://{cluster}.{schema}/{tbl}/{col}/{description_id}'

    # Relation between column and tag
    COL_TAG_RELATION_TYPE = 'TAGGED_BY'
    TAG_COL_RELATION_TYPE = 'TAG'

    def __init__(self,
                 name,  # type: str
                 description,  # type: Union[str, None]
                 col_type,  # type: str
                 sort_order,  # type: int
                 tags=None,  # type: Union[List[str], None],
                 description_source=None  # type: Union[str, None]
                 ):
        # type: (...) -> None
        """
        TODO: Add stats
        :param name:
        :param description:
        :param col_type:
        :param sort_order:
        """
        self.name = name
        self.description = DescriptionMetadata.create_description_metadata(source=description_source,
                                                                           text=description)
        self.type = col_type
        self.sort_order = sort_order
        self.tags = tags

    def __repr__(self):
        # type: () -> str
        return 'ColumnMetadata({!r}, {!r}, {!r}, {!r})'.format(self.name,
                                                               self.description,
                                                               self.type,
                                                               self.sort_order)


class TableMetadata(Neo4jCsvSerializable):
    """
    Table metadata that contains columns. It implements Neo4jCsvSerializable so that it can be serialized to produce
    Table, Column and relation of those along with relationship with table and schema. Additionally, it will create
    Database, Cluster, and Schema with relastionships between those.
    These are being created here as it does not make much sense to have different extraction to produce this. As
    database, cluster, schema would be very repititive with low cardinality, it will perform de-dupe so that publisher
    won't need to publish same nodes, relationships.

    This class can be used for both table and view metadata. If it is a View, is_view=True should be passed in.
    """
    TABLE_NODE_LABEL = 'Table'
    TABLE_KEY_FORMAT = '{db}://{cluster}.{schema}/{tbl}'
    TABLE_NAME = 'name'
    IS_VIEW = 'is_view'

    TABLE_DESCRIPTION_FORMAT = '{db}://{cluster}.{schema}/{tbl}/{description_id}'

    DATABASE_NODE_LABEL = 'Database'
    DATABASE_KEY_FORMAT = 'database://{db}'
    DATABASE_CLUSTER_RELATION_TYPE = cluster_constants.CLUSTER_RELATION_TYPE
    CLUSTER_DATABASE_RELATION_TYPE = cluster_constants.CLUSTER_REVERSE_RELATION_TYPE

    CLUSTER_NODE_LABEL = cluster_constants.CLUSTER_NODE_LABEL
    CLUSTER_KEY_FORMAT = '{db}://{cluster}'
    CLUSTER_SCHEMA_RELATION_TYPE = schema_constant.SCHEMA_RELATION_TYPE
    SCHEMA_CLUSTER_RELATION_TYPE = schema_constant.SCHEMA_REVERSE_RELATION_TYPE

    SCHEMA_NODE_LABEL = schema_constant.SCHEMA_NODE_LABEL
    SCHEMA_KEY_FORMAT = schema_constant.DATABASE_SCHEMA_KEY_FORMAT
    SCHEMA_TABLE_RELATION_TYPE = 'TABLE'
    TABLE_SCHEMA_RELATION_TYPE = 'TABLE_OF'

    TABLE_COL_RELATION_TYPE = 'COLUMN'
    COL_TABLE_RELATION_TYPE = 'COLUMN_OF'

    TABLE_TAG_RELATION_TYPE = 'TAGGED_BY'
    TAG_TABLE_RELATION_TYPE = 'TAG'

    # Only for deduping database, cluster, and schema (table and column will be always processed)
    serialized_nodes_ids = set()  # type: Set[str]
    serialized_rels_ids = set()  # type: Set[Tuple[str]]

    def __init__(self,
                 database,  # type: str
                 cluster,  # type: str
                 schema,  # type: str
                 name,  # type: str
                 description,  # type: Union[str, None]
                 columns=None,  # type: Iterable[ColumnMetadata]
                 is_view=False,  # type: bool
                 tags=None,  # type: Union[List, str]
                 description_source=None,  # type: Union[str, None]
                 **kwargs  # type: Dict
                 ):
        # type: (...) -> None
        """
        :param database:
        :param cluster:
        :param schema:
        :param name:
        :param description:
        :param columns:
        :param is_view: Indicate whether the table is a view or not
        :param tags:
        :param description_source: Optional. Where the description is coming from. Used to compose unique id.
        :param kwargs: Put additional attributes to the table model if there is any.
        """
        self.database = database
        self.cluster = cluster
        self.schema = schema
        self.name = name
        self.description = DescriptionMetadata.create_description_metadata(text=description, source=description_source)
        self.columns = columns if columns else []
        self.is_view = is_view
        self.attrs = None

        self.tags = TableMetadata.format_tags(tags)

        if kwargs:
            self.attrs = copy.deepcopy(kwargs)

        self._node_iterator = self._create_next_node()
        self._relation_iterator = self._create_next_relation()

    def __repr__(self):
        # type: () -> str
        return 'TableMetadata({!r}, {!r}, {!r}, {!r} ' \
               '{!r}, {!r}, {!r}, {!r})'.format(self.database,
                                                self.cluster,
                                                self.schema,
                                                self.name,
                                                self.description,
                                                self.columns,
                                                self.is_view,
                                                self.tags)

    def _get_table_key(self):
        # type: () -> str
        return TableMetadata.TABLE_KEY_FORMAT.format(db=self.database,
                                                     cluster=self.cluster,
                                                     schema=self.schema,
                                                     tbl=self.name)

    def _get_table_description_key(self, description):
        # type: (DescriptionMetadata) -> str
        return TableMetadata.TABLE_DESCRIPTION_FORMAT.format(db=self.database,
                                                             cluster=self.cluster,
                                                             schema=self.schema,
                                                             tbl=self.name,
                                                             description_id=description.get_description_id())

    def _get_database_key(self):
        # type: () -> str
        return TableMetadata.DATABASE_KEY_FORMAT.format(db=self.database)

    def _get_cluster_key(self):
        # type: () -> str
        return TableMetadata.CLUSTER_KEY_FORMAT.format(db=self.database,
                                                       cluster=self.cluster)

    def _get_schema_key(self):
        # type: () -> str
        return TableMetadata.SCHEMA_KEY_FORMAT.format(db=self.database,
                                                      cluster=self.cluster,
                                                      schema=self.schema)

    def _get_col_key(self, col):
        # type: (ColumnMetadata) -> str
        return ColumnMetadata.COLUMN_KEY_FORMAT.format(db=self.database,
                                                       cluster=self.cluster,
                                                       schema=self.schema,
                                                       tbl=self.name,
                                                       col=col.name)

    def _get_col_description_key(self, col, description):
        # type: (ColumnMetadata, DescriptionMetadata) -> str
        return ColumnMetadata.COLUMN_DESCRIPTION_FORMAT.format(db=self.database,
                                                               cluster=self.cluster,
                                                               schema=self.schema,
                                                               tbl=self.name,
                                                               col=col.name,
                                                               description_id=description.get_description_id())

    @staticmethod
    def format_tags(tags):
        if isinstance(tags, string_types):
            tags = list(filter(None, tags.split(',')))
        if isinstance(tags, list):
            tags = [tag.lower().strip() for tag in tags]
        return tags

    def create_next_node(self):
        # type: () -> Union[GraphNode, None]
        try:
            return next(self._node_iterator)
        except StopIteration:
            return None

    def _create_next_node(self):  # noqa: C901
        # type: () -> Iterator[GraphNode]

        table_attributes = {
            TableMetadata.TABLE_NAME: self.name,
            TableMetadata.IS_VIEW: self.is_view
        }
        if self.attrs:
            for k, v in self.attrs.items():
                if k not in table_attributes:
                    table_attributes[k] = v

        table_node = GraphNode(
            id=self._get_table_key(),
            label=TableMetadata.TABLE_NODE_LABEL,
            node_attributes=table_attributes
        )
        yield table_node

        if self.description:
            node_key = self._get_table_description_key(self.description)
            yield self.description.get_node(node_key)

        # Create the table tag node
        if self.tags:
            for tag in self.tags:
                yield TagMetadata.create_tag_node(tag)

        for col in self.columns:
            column_node = GraphNode(
                id=self._get_col_key(col),
                label=ColumnMetadata.COLUMN_NODE_LABEL,
                node_attributes={
                    ColumnMetadata.COLUMN_NAME: col.name,
                    ColumnMetadata.COLUMN_TYPE: col.type,
                    ColumnMetadata.COLUMN_ORDER: col.sort_order
                }
            )
            yield column_node

            if col.description:
                node_key = self._get_col_description_key(col, col.description)
                yield col.description.get_node(node_key)

            if col.tags:
                for tag in col.tags:
                    tag_node = GraphNode(
                        id=TagMetadata.get_tag_key(tag),
                        label=TagMetadata.TAG_NODE_LABEL,
                        node_attributes={
                            TagMetadata.TAG_TYPE: 'default'
                        }
                    )
                    yield tag_node

        # Database, cluster, schema
        others = [
            GraphNode(
                id=self._get_database_key(),
                label=TableMetadata.DATABASE_NODE_LABEL,
                node_attributes={
                    'name': self.database
                }
            ),
            GraphNode(
                id=self._get_cluster_key(),
                label=TableMetadata.CLUSTER_NODE_LABEL,
                node_attributes={
                    'name': self.cluster
                }
            ),
            GraphNode(
                id=self._get_schema_key(),
                label=TableMetadata.SCHEMA_NODE_LABEL,
                node_attributes={
                    'name': self.schema
                }
            )
        ]

        for node_tuple in others:
            if node_tuple.id not in TableMetadata.serialized_nodes_ids:
                TableMetadata.serialized_nodes_ids.add(node_tuple.id)
                yield node_tuple

    def create_next_relation(self):
        # type: () -> Union[GraphRelationship, None]
        try:
            return next(self._relation_iterator)
        except StopIteration:
            return None

    def _create_next_relation(self):
        # type: () -> Iterator[GraphRelationship]

        schema_table_relationship = GraphRelationship(
            start_key=self._get_schema_key(),
            start_label=TableMetadata.SCHEMA_NODE_LABEL,
            end_key=self._get_table_key(),
            end_label=TableMetadata.TABLE_NODE_LABEL,
            type=TableMetadata.SCHEMA_TABLE_RELATION_TYPE,
            reverse_type=TableMetadata.TABLE_SCHEMA_RELATION_TYPE,
            relationship_attributes={}
        )
        yield schema_table_relationship

        if self.description:
            yield self.description.get_relation(TableMetadata.TABLE_NODE_LABEL,
                                                self._get_table_key(),
                                                self._get_table_description_key(self.description))

        if self.tags:
            for tag in self.tags:
                tag_relationship = GraphRelationship(
                    start_label=TableMetadata.TABLE_NODE_LABEL,
                    start_key=self._get_table_key(),
                    end_label=TagMetadata.TAG_NODE_LABEL,
                    end_key=TagMetadata.get_tag_key(tag),
                    type=TableMetadata.TABLE_TAG_RELATION_TYPE,
                    reverse_type=TableMetadata.TAG_TABLE_RELATION_TYPE,
                    relationship_attributes={}
                )
                yield tag_relationship

        for col in self.columns:
            column_relationship = GraphRelationship(
                start_label=TableMetadata.TABLE_NODE_LABEL,
                start_key=self._get_table_key(),
                end_label=ColumnMetadata.COLUMN_NODE_LABEL,
                end_key=self._get_col_key(col),
                type=TableMetadata.TABLE_COL_RELATION_TYPE,
                reverse_type=TableMetadata.COL_TABLE_RELATION_TYPE,
                relationship_attributes={}
            )
            yield column_relationship

            if col.description:
                yield col.description.get_relation(
                    ColumnMetadata.COLUMN_NODE_LABEL,
                    self._get_col_key(col),
                    self._get_col_description_key(col, col.description)
                )

            if col.tags:
                for tag in col.tags:
                    tag_column_relationship = GraphRelationship(
                        start_label=TableMetadata.TABLE_NODE_LABEL,
                        end_label=TagMetadata.TAG_NODE_LABEL,
                        start_key=self._get_table_key(),
                        end_key=TagMetadata.get_tag_key(tag),
                        type=ColumnMetadata.COL_TAG_RELATION_TYPE,
                        reverse_type=ColumnMetadata.TAG_COL_RELATION_TYPE,
                        relationship_attributes={}
                    )
                    yield tag_column_relationship

        others = [
            GraphRelationship(
                start_label=TableMetadata.DATABASE_NODE_LABEL,
                end_label=TableMetadata.CLUSTER_NODE_LABEL,
                start_key=self._get_database_key(),
                end_key=self._get_cluster_key(),
                type=TableMetadata.DATABASE_CLUSTER_RELATION_TYPE,
                reverse_type=TableMetadata.CLUSTER_DATABASE_RELATION_TYPE,
                relationship_attributes={}
            ),
            GraphRelationship(
                start_label=TableMetadata.CLUSTER_NODE_LABEL,
                end_label=TableMetadata.SCHEMA_NODE_LABEL,
                start_key=self._get_cluster_key(),
                end_key=self._get_schema_key(),
                type=TableMetadata.CLUSTER_SCHEMA_RELATION_TYPE,
                reverse_type=TableMetadata.SCHEMA_CLUSTER_RELATION_TYPE,
                relationship_attributes={}
            )
        ]

        for rel_tuple in others:
            if (rel_tuple.start_key, rel_tuple.end_key, rel_tuple.type) not in TableMetadata.serialized_rels_ids:
                TableMetadata.serialized_rels_ids.add((rel_tuple.start_key, rel_tuple.end_key, rel_tuple.type))
                yield rel_tuple
