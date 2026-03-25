"""Hello World PyFlink job — minimal batch example."""

from pyflink.table import EnvironmentSettings, TableEnvironment


def run():
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
    result.execute().print()


if __name__ == "__main__":
    run()
