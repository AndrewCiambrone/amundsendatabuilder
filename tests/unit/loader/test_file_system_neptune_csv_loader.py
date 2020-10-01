import unittest
import os
from os import listdir
from os.path import isfile, join
from collections import OrderedDict
from csv import DictReader
from freezegun import freeze_time

from databuilder.loader.file_system_neptune_csv_loader import FSNeptuneCSVLoader
from tests.unit.models.test_neo4j_csv_serde import Movie, Actor, City
from databuilder.job.base_job import Job
from pyhocon import ConfigFactory
from operator import itemgetter
from mock import patch
from typing import Dict, Iterable, Any, Callable  # noqa: F401


class FileSystemNeptuneCSVLoaderTest(unittest.TestCase):
    def setUp(self) -> None:
        prefix = '/var/tmp/TestFileSystemNeptuneCSVLoader'
        self._conf = ConfigFactory.from_dict(
            {
                FSNeptuneCSVLoader.NODE_DIR_PATH: '{}/{}'.format(prefix, 'nodes'),
                FSNeptuneCSVLoader.RELATION_DIR_PATH: '{}/{}'.format(prefix, 'relationships'),
                FSNeptuneCSVLoader.SHOULD_DELETE_CREATED_DIR: True,
                FSNeptuneCSVLoader.FORCE_CREATE_DIR: True
            }
        )

    def tearDown(self) -> None:
        Job.closer.close()

    @freeze_time("2020-09-01 01:01:00")
    def test_load(self):
        actors = [Actor('Tom Cruise'), Actor('Meg Ryan')]
        cities = [City('San Diego'), City('Oakland')]
        movie = Movie('Top Gun', actors, cities)

        loader = FSNeptuneCSVLoader()
        loader.init(self._conf)
        loader.load(movie)

        loader.close()

        expected_node_path = '{}/../resources/fs_neptune_csv_loader/nodes'.format(os.path.join(os.path.dirname(__file__)))
        expected_nodes = self._get_csv_rows(
            expected_node_path,
            itemgetter('~id')
        )
        actual_nodes = self._get_csv_rows(
            self._conf.get_string(FSNeptuneCSVLoader.NODE_DIR_PATH),
            itemgetter('~id')
        )
        self.assertEqual(expected_nodes, actual_nodes)

        expected_rel_path = '{}/../resources/fs_neptune_csv_loader/relationships'.format(
            os.path.join(os.path.dirname(__file__))
        )
        expected_relations = self._get_csv_rows(
            expected_rel_path,
            itemgetter('~id')
        )
        actual_relations = self._get_csv_rows(
            self._conf.get_string(FSNeptuneCSVLoader.RELATION_DIR_PATH),
            itemgetter('~id')
        )
        self.assertEqual(expected_relations, actual_relations)

    def _get_csv_rows(self, path, sorting_key_getter):
        # type: (str, Callable) -> Iterable[Dict[str, Any]]
        files = [join(path, f) for f in listdir(path) if isfile(join(path, f))]

        result = []
        for f in files:
            with open(f, 'r') as f_input:
                reader = DictReader(f_input)
                for row in reader:
                    result.append(OrderedDict(sorted(row.items())))

        return sorted(result, key=sorting_key_getter)


if __name__ == '__main__':
    unittest.main()
