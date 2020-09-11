import unittest

from typing import Union, Dict, Any, Iterable  # noqa: F401

from databuilder.models.neo4j_csv_serde import (  # noqa: F401
    NODE_KEY, NODE_LABEL, RELATION_START_KEY, RELATION_START_LABEL,
    RELATION_END_KEY, RELATION_END_LABEL, RELATION_TYPE,
    RELATION_REVERSE_TYPE)
from databuilder.models.neo4j_csv_serde import Neo4jCsvSerializable
from databuilder.models.graph_node import GraphNode
from databuilder.models.graph_relationship import GraphRelationship


class TestSerialize(unittest.TestCase):

    def test_serialize(self):
        # type: () -> None
        actors = [Actor('Tom Cruise'), Actor('Meg Ryan')]
        cities = [City('San Diego'), City('Oakland')]
        movie = Movie('Top Gun', actors, cities)

        actual = []
        node_row = movie.next_node()
        while node_row:
            actual.append(node_row)
            node_row = movie.next_node()

        expected = [
            {'name': 'Top Gun', 'KEY': 'movie://Top Gun', 'LABEL': 'Movie'},
            {'name': 'Top Gun', 'KEY': 'actor://Tom Cruise', 'LABEL': 'Actor'},
            {'name': 'Top Gun', 'KEY': 'actor://Meg Ryan', 'LABEL': 'Actor'},
            {'name': 'Top Gun', 'KEY': 'city://San Diego', 'LABEL': 'City'},
            {'name': 'Top Gun', 'KEY': 'city://Oakland', 'LABEL': 'City'}
        ]
        self.assertEqual(expected, actual)

        actual = []
        relation_row = movie.next_relation()
        while relation_row:
            actual.append(relation_row)
            relation_row = movie.next_relation()

        expected = [
            {'END_KEY': 'actor://Tom Cruise', 'START_LABEL': 'Movie',
             'END_LABEL': 'Actor', 'START_KEY': 'movie://Top Gun',
             'TYPE': 'ACTOR', 'REVERSE_TYPE': 'ACTED_IN'},
            {'END_KEY': 'actor://Meg Ryan', 'START_LABEL': 'Movie',
             'END_LABEL': 'Actor', 'START_KEY': 'movie://Top Gun',
             'TYPE': 'ACTOR', 'REVERSE_TYPE': 'ACTED_IN'},
            {'END_KEY': 'city://San Diego', 'START_LABEL': 'Movie',
             'END_LABEL': 'City', 'START_KEY': 'city://Top Gun',
             'TYPE': 'FILMED_AT', 'REVERSE_TYPE': 'APPEARS_IN'},
            {'END_KEY': 'city://Oakland', 'START_LABEL': 'Movie',
             'END_LABEL': 'City', 'START_KEY': 'city://Top Gun',
             'TYPE': 'FILMED_AT', 'REVERSE_TYPE': 'APPEARS_IN'}
        ]
        self.assertEqual(expected, actual)


class Movie(Neo4jCsvSerializable):
    LABEL = 'Movie'
    KEY_FORMAT = 'movie://{}'
    MOVIE_ACTOR_RELATION_TYPE = 'ACTOR'
    ACTOR_MOVIE_RELATION_TYPE = 'ACTED_IN'
    MOVIE_CITY_RELATION_TYPE = 'FILMED_AT'
    CITY_MOVIE_RELATION_TYPE = 'APPEARS_IN'

    def __init__(self, name, actors, cities):
        # type: (str, Iterable[Actor], Iterable[City]) -> None
        self._name = name
        self._actors = actors
        self._cities = cities
        self._node_iter = iter(self.create_nodes())
        self._relation_iter = iter(self.create_relation())

    def create_next_node(self):
        # type: () -> Union[Dict[str, Any], None]
        try:
            return next(self._node_iter)
        except StopIteration:
            return None

    def create_next_relation(self):
        # type: () -> Union[Dict[str, Any], None]
        try:
            return next(self._relation_iter)
        except StopIteration:
            return None

    def create_nodes(self):
        # type: () -> Iterable[Dict[str, Any]]
        result = [
            GraphNode(
                id=Movie.KEY_FORMAT.format(self._name),
                label=Movie.LABEL,
                node_attributes={
                    'name': self._name
                }
            )
        ]

        for actor in self._actors:
            actor_node = GraphNode(
                id=Actor.KEY_FORMAT.format(actor.name),
                label=Actor.LABEL,
                node_attributes={
                    'name': self._name
                }
            )
            result.append(actor_node)

        for city in self._cities:
            city_node = GraphNode(
                id=City.KEY_FORMAT.format(city.name),
                label=City.LABEL,
                node_attributes={
                    'name': self._name
                }
            )
            result.append(city_node)
        return result

    def create_relation(self):
        # type: () -> Iterable[Dict[str, Any]]
        result = []
        for actor in self._actors:
            actor_relation = GraphRelationship(
                start_key=Movie.KEY_FORMAT.format(self._name),
                start_label=Movie.LABEL,
                end_key=Actor.KEY_FORMAT.format(actor.name),
                end_label=Actor.LABEL,
                type=Movie.MOVIE_ACTOR_RELATION_TYPE,
                reverse_type=Movie.ACTOR_MOVIE_RELATION_TYPE,
                relationship_attributes={}
            )
            result.append(actor_relation)

        for city in self._cities:
            city_relation = GraphRelationship(
                start_key=Movie.KEY_FORMAT.format(self._name),
                start_label=Movie.LABEL,
                end_key=City.KEY_FORMAT.format(city.name),
                end_label=City.LABEL,
                type=Movie.MOVIE_CITY_RELATION_TYPE,
                reverse_type=Movie.CITY_MOVIE_RELATION_TYPE,
                relationship_attributes={}
            )
            result.append(city_relation)
        return result


class Actor(object):
    LABEL = 'Actor'
    KEY_FORMAT = 'actor://{}'

    def __init__(self, name):
        # type: (str) -> None
        self.name = name


class City(object):
    LABEL = 'City'
    KEY_FORMAT = 'city://{}'

    def __init__(self, name):
        # type: (str) -> None
        self.name = name


if __name__ == '__main__':
    unittest.main()
