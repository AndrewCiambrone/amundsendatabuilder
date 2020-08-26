from databuilder import neptune_client
import os
from gremlin_python.process.traversal import T, Column
from gremlin_python.process import traversal
from gremlin_python.process.graph_traversal import __
from datetime import datetime, timedelta


def remove_stale_data():
    access_key = os.getenv('AWS_KEY')
    access_secret = os.getenv('AWS_SECRET_KEY')
    aws_zone = os.getenv("AWS_ZONE")
    neptune_endpoint = os.getenv('NEPTUNE_ENDPOINT')
    neptune_port = os.getenv("NEPTUNE_PORT")
    neptune_host = "wss://{}:{}/gremlin".format(neptune_endpoint, neptune_port)
    auth_dict = {
        'aws_access_key_id': access_key,
        'aws_secret_access_key': access_secret,
        'service_region': aws_zone
    }
    yesterday = datetime.utcnow() - timedelta(days=1)
    filter_properties = [()]
    g = neptune_client.get_graph(
        host=neptune_host,
        password=auth_dict
    )
    test = g.V().groupCount().by(T.label).unfold().project('Entity Type', 'Count').by(Column.keys).by(Column.values)
    print(g)


if __name__ == '__main__':
    remove_stale_data()