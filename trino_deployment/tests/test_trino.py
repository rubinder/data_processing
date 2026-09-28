"""Against the running stack: Trino, Iceberg REST on the S3 gateway, and PostgreSQL
with the stage roles applied and gold promoted. Skips without Trino."""
import pytest

from trino_deployment import analyses, client, federation, pushdown, seed, timetravel

pytestmark = pytest.mark.trino


def _gold_ready(conn) -> bool:
    try:
        return client.query("SELECT count(*) FROM postgres_gold.gold.page_type_summary",
                            conn).rows[0][0] > 0
    except Exception:  # noqa: BLE001
        return False


@pytest.fixture(scope="session")
def seeded(conn):
    if not _gold_ready(conn):
        pytest.skip("gold not promoted in PostgreSQL; run ../mcp_deployment/deploy.sh demo")
    seed.create(conn)
    seed.truncate(conn)
    n = seed.from_postgres(conn)
    assert n > 0
    return n


def test_the_three_catalogs_are_mounted(conn):
    names = {r[0] for r in client.query("SHOW CATALOGS", conn).rows}
    assert {"iceberg", "postgres_raw", "postgres_gold"} <= names


def test_seed_from_raw_lands_the_parseable_rows(conn, seeded):
    raw_total = client.query("SELECT count(*) FROM postgres_raw.raw.impressions", conn).rows[0][0]
    bad = client.query("SELECT count(*) FROM postgres_raw.raw.impressions "
                       "WHERE try_cast(date AS date) IS NULL", conn).rows[0][0]
    assert seed.row_count(conn) == raw_total - bad == seeded


def test_analyses_return_the_gold_columns_and_a_monotonic_funnel(conn, seeded):
    funnel = analyses.run("funnel_analysis", conn)
    assert funnel.columns == ["page_type", "event_type", "impressions_at_stage",
                              "total_impressions", "pct_of_total", "pct_from_previous_stage"]
    by_page: dict[int, list[int]] = {}
    for row in funnel.dicts():
        by_page.setdefault(row["page_type"], []).append(row["impressions_at_stage"])
    for counts in by_page.values():
        assert counts == sorted(counts, reverse=True)
    for name in ("page_type_summary", "user_engagement", "hourly_traffic"):
        assert analyses.run(name, conn).rows, name


def test_trino_over_iceberg_reconciles_to_dbt_gold_exactly(conn, seeded):
    rows = federation.reconcile(conn)
    assert len(rows) == 3
    assert federation.mismatches(rows) == []


def test_least_privilege_holds_through_federation(conn):
    with pytest.raises(Exception, match="permission denied"):
        client.query("SELECT count(*) FROM postgres_gold.raw.impressions", conn)
    with pytest.raises(Exception, match="permission denied"):
        client.query("SELECT count(*) FROM postgres_raw.gold.page_type_summary", conn)


def test_time_travel_reads_the_previous_snapshot(conn, seeded):
    before = seed.row_count(conn)
    seed.synthetic(10, seed_value=77, conn=conn)
    snaps = timetravel.snapshots(conn=conn)
    assert len(snaps) >= 2 and snaps[-1]["operation"] == "append"
    assert timetravel.count_at(snaps[-2]["snapshot_id"], conn=conn) == before
    assert seed.row_count(conn) > before
    client.query(f"DELETE FROM {seed.TABLE} WHERE impression_id LIKE 'b77_%'", conn)
    assert seed.row_count(conn) == before
    assert timetravel.partitions(conn=conn)


def test_jdbc_pushdown_is_visible_in_the_plan(conn, seeded):
    assert pushdown.plan_is_pushed(client.explain(pushdown.PUSHED, conn))
    assert not pushdown.plan_is_pushed(client.explain(pushdown.NOT_PUSHED, conn))
    assert client.query(pushdown.PUSHED, conn).rows == client.query(pushdown.NOT_PUSHED, conn).rows
