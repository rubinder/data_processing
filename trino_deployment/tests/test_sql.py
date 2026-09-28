"""No Trino needed: the SQL is inspected, and the plan classifier is exercised."""
from trino_deployment import analyses, federation, pushdown, seed


def test_every_analysis_reads_the_iceberg_table_and_only_it():
    for name, sql in analyses.ANALYSES.items():
        assert seed.TABLE in sql, name
        assert "postgres" not in sql.lower(), name


def test_seed_from_postgres_skips_unparseable_dates_like_dbt_does():
    assert "try_cast(date AS date) IS NOT NULL" in seed.FROM_POSTGRES
    assert "postgres_raw.raw.impressions" in seed.FROM_POSTGRES
    assert "partitioning = ARRAY['day(event_ts)', 'page_type']" in seed.CREATE_TABLE


def test_reconciliation_covers_every_gold_metric_from_both_sides():
    sql = federation.reconciliation_sql()
    assert "FULL OUTER JOIN postgres_gold.gold.page_type_summary" in sql
    for metric in federation.METRICS:
        assert f"delta_{metric}" in sql and f"iceberg_{metric}" in sql and f"gold_{metric}" in sql


def test_mismatches_reports_only_nonzero_or_missing_deltas():
    rows = [{"page_type": 1, **{f"delta_{m}": 0.0 for m in federation.METRICS}},
            {"page_type": 2, **{f"delta_{m}": 0.0 for m in federation.METRICS},
             "delta_unique_users": 3.0, "delta_pct_reaching_f": None}]
    assert federation.mismatches(rows) == [(2, "unique_users", 3.0), (2, "pct_reaching_f", None)]


def test_plan_classifier_distinguishes_pushed_from_filtered_plans():
    pushed = "Fragment 0 [SINGLE]\n Output[columnNames = [_col0]]\n  TableScan[table = postgres_gold:Query[...]]"
    filtered = ("Fragment 1 [SOURCE]\n Aggregate[type = PARTIAL]\n  ScanFilterProject[table = ..., "
                "filterPredicate = (CAST(page_type AS varchar) = VARCHAR '3')]")
    assert pushdown.plan_is_pushed(pushed)
    assert not pushdown.plan_is_pushed(filtered)
