"""Templates on the Trino engine, against the running stack. Skips otherwise."""
import pytest

from mcp_deployment import templates, trino_engine
from mcp_deployment.templates import TemplateError

pytestmark = [pytest.mark.postgres, pytest.mark.trino]


@pytest.fixture(scope="module")
def trino_up(admin):
    """Trino reachable, and gold pointing at the real dbt output rather than
    at whatever candidate schema an earlier test in this session promoted."""
    if not trino_engine.reachable():
        pytest.skip("Trino not reachable; ../trino_deployment/deploy.sh up")
    from mcp_deployment import config
    has_real = admin.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = %s "
        "AND table_type = 'BASE TABLE'", (config.SOURCE_SCHEMA,)).fetchone()[0]
    if not has_real:
        pytest.skip("public_analytics is empty; run ../dbt_deployment/deploy.sh run")
    admin.execute("SELECT gold_ops.promote(%s, %s)", (config.SOURCE_SCHEMA, "trino tests"))


def test_reconciliation_template_runs_on_trino_and_binds_a_null_optional(trino_up):
    loaded = templates.load_templates()
    out = trino_engine.execute(loaded["reconcile_page_type_summary"], {})
    assert out.columns[:3] == ["page_type", "gold_total_impressions", "iceberg_total_impressions"]
    assert out.row_count == 3 and out.params == {"page_type": None}
    one = trino_engine.execute(loaded["reconcile_page_type_summary"], {"page_type": 2})
    assert [r[0] for r in one.rows] == [2]


def test_clickhouse_template_reads_the_cluster_through_trino(trino_up):
    loaded = templates.load_templates()
    out = trino_engine.execute(loaded["clickhouse_daily_volume"], {"days": 3})
    assert out.columns == ["date", "page_type", "impressions", "unique_users"]
    assert out.row_count >= 1


def test_reader_user_is_read_only_at_the_trino_layer(trino_up):
    import trino.exceptions
    conn = trino_engine.connect(5000)
    cur = conn.cursor()
    with pytest.raises(trino.exceptions.TrinoUserError, match="Access Denied"):
        cur.execute("INSERT INTO iceberg.db.impressions VALUES "
                    "('u', 'x', 1, TIMESTAMP '2026-01-01 00:00:00', 'a')")
        cur.fetchall()
    conn.close()


def test_a_trino_template_that_times_out_is_a_template_error(trino_up):
    slow = templates.Template("slow", "slow probe", "SELECT count(*) FROM iceberg.db.impressions "
                              "CROSS JOIN iceberg.db.impressions CROSS JOIN iceberg.db.impressions",
                              timeout_ms=1000, engine="trino")
    with pytest.raises(TemplateError, match="timeout"):
        trino_engine.execute(slow, {})
