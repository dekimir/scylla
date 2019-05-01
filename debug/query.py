# Useful debug utilities for querying Cassandra or ScyllaDB without timeouts.

from cassandra.cluster import Cluster


def connect(namespace=None, **kwargs):
    """(Re)opens connection to the database and sets the global session.

    kwargs is passed to Cluster constructor.

    """
    global cassandra_session
    cluster = Cluster(control_connection_timeout=None, idle_heartbeat_interval=0, **kwargs)
    if namespace is not None:
        cassandra_session = cluster.connect(namespace)
    else:
        cassandra_session = cluster.connect()


def query(q):
    """Returns the result of executing q via the global session."""
    return list(cassandra_session.execute(q, timeout=None))
