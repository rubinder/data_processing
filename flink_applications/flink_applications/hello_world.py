"""Hello World PyFlink job — minimal batch example."""

from pyflink.table import EnvironmentSettings, TableEnvironment


def run() -> list[tuple]:
    """Run the job and return the collected rows.

    Returning the rows (rather than only printing them) is what makes the
    job testable: ``TableResult.print()`` writes from the JVM, which pytest's
    ``capsys`` never sees, so a test asserting on captured stdout fails even
    when the job succeeds.
    """
    env_settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(env_settings)

    source_ddl = """
        CREATE TABLE hello_source (
            id INT,
            message STRING
        ) WITH (
            'connector' = 'datagen',
            'number-of-rows' = '10'
        )
    """
    t_env.execute_sql(source_ddl)

    result = t_env.sql_query("SELECT id, message FROM hello_source")
    with result.execute().collect() as rows:
        collected = [tuple(row) for row in rows]
    for row in collected:
        print(row)
    return collected


if __name__ == "__main__":
    run()
