import logging
import six
from collections import namedtuple

from pyhocon import ConfigFactory, ConfigTree  # noqa: F401
from typing import Iterator, Union, Dict, Any  # noqa: F401
import sqlalchemy

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata
from databuilder.models.user import User
from itertools import groupby


TableKey = namedtuple('TableKey', ['schema', 'table_name'])

LOGGER = logging.getLogger(__name__)


class PostgresUserExtractor(Extractor):
    """
    Extracts Postgres users using SQLAlchemyExtractor
    """



    # CONFIG KEYS
    SQL_STATEMENT_KEY = 'sql_statement_key'

    def init(self, conf):
        # type: (ConfigTree) -> None

        self.sql_stmt = conf.get_string(PostgresUserExtractor.SQL_STATEMENT_KEY)
        self.sql_stmt = self.sql_stmt.replace('%', '%%')

        self._alchemy_extractor = SQLAlchemyExtractor()
        sql_alch_conf = Scoped.get_scoped_conf(conf, self._alchemy_extractor.get_scope()) \
            .with_fallback(ConfigFactory.from_dict({SQLAlchemyExtractor.EXTRACT_SQL: self.sql_stmt}))

        self.sql_stmt = sql_alch_conf.get_string(SQLAlchemyExtractor.EXTRACT_SQL)

        LOGGER.info('SQL for postgres metadata: {}'.format(self.sql_stmt))

        self._alchemy_extractor.init(sql_alch_conf)
        self._extract_iter = None  # type: Union[None, Iterator]

    def extract(self):
        # type: () -> Union[TableMetadata, None]
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self):
        # type: () -> str
        return 'extractor.postgres_users'

    def _get_extract_iter(self):
        # type: () -> Iterator[User]
        """
        :return:
        """
        for row in self._get_raw_extract_iter():
            (user_name, email) = row
            first, last = user_name.split("_", 1)
            if not email:
                email = "{user_name}@seatgeek.com".format(user_name=user_name.strip())

            yield User(
                email,
                first_name=first.capitalize(),
                last_name=last.capitalize(),
                do_not_update_empty_attribute=True
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