import logging
from collections import namedtuple

from pyhocon import ConfigFactory, ConfigTree  # noqa: F401
from typing import Iterator, Union, Dict, Any  # noqa: F401

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.models.column_usage_model import ColumnUsageModel

LOGGER = logging.getLogger(__name__)


class PostgresTableUsageExtractor(Extractor):
    """
    Extracts Postgres table usage using SQLAlchemyExtractor
    """

    # CONFIG KEYS
    SQL_STATEMENT_KEY = 'sql_statement_key'

    def init(self, conf):
        # type: (ConfigTree) -> None

        self.sql_stmt = conf.get_string(PostgresTableUsageExtractor.SQL_STATEMENT_KEY)
        self.sql_stmt = self.sql_stmt.replace('%', '%%')

        self._alchemy_extractor = SQLAlchemyExtractor()
        sql_alch_conf = Scoped.get_scoped_conf(conf, self._alchemy_extractor.get_scope()) \
            .with_fallback(ConfigFactory.from_dict({SQLAlchemyExtractor.EXTRACT_SQL: self.sql_stmt}))

        self.sql_stmt = sql_alch_conf.get_string(SQLAlchemyExtractor.EXTRACT_SQL)

        LOGGER.info('SQL for postgres metadata: {}'.format(self.sql_stmt))

        self._alchemy_extractor.init(sql_alch_conf)
        self._extract_iter = None  # type: Union[None, Iterator]

    def extract(self):
        # type: () -> Union[User, None]
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self):
        # type: () -> str
        return 'extractor.postgres_table_usage'

    def _get_extract_iter(self):
        # type: () -> Iterator[User]
        """
        :return:
        """
        for row in self._get_raw_extract_iter():
            (database_name, schema_name, table_name, user_name, total_queries_count) = row

            yield ColumnUsageModel(
                database=database_name,
                cluster=database_name,
                schema=schema_name,
                table_name=table_name,
                column_name="*",
                user_email=user_name,
                read_count=total_queries_count
            )

    def _get_raw_extract_iter(self):
        # type: () -> Iterator[Dict[str, Any]]
        """
        Provides iterator of result row from SQLAlchemy extractor
        :return:
        """
        row = self._alchemy_extractor.extract()
        while row:
            yield row
            row = self._alchemy_extractor.extract()
