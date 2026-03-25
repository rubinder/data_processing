"""Tests for the hello_world Flink job."""

from flink_applications.hello_world import run


def test_hello_world_runs(capsys):
    """Verify the hello world job completes without error."""
    run()
    captured = capsys.readouterr()
    assert captured.out, "Expected output from hello_world job"
