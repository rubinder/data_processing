"""Tests for the hello_world Flink job."""

from flink_applications.hello_world import run


def test_hello_world_returns_ten_rows():
    """The job runs on the local mini-cluster and yields the datagen rows.

    Asserts on the collected result, not on stdout: the Table API prints
    from the JVM, outside pytest's capture.
    """
    rows = run()
    assert len(rows) == 10
    for row_id, message in rows:
        assert isinstance(row_id, int)
        assert isinstance(message, str)
