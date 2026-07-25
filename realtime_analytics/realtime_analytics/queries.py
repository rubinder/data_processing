"""The four queries the pipeline exists to serve, in every schema variant.

Each query is written three ways so the benchmark compares like with like:

``naive``  against the all-String / JSON-blob table (stage 0)
``typed``  against any typed events table (stages 1-4, 6) -- identical text,
           only the table name changes, so the measured difference is purely
           physical layout
``mv``     against the pre-aggregated materialized view targets (stage 5)

Every variant computes the *same* numbers, and the benchmark asserts that
they agree before it reports a speedup. A faster query that returns different
results is not an optimization.

One deliberate constraint: even the naive variant uses quantileTDigest, not
quantileExact. If the naive stage used a heavier aggregate function, part of
the reported speedup would come from swapping the function rather than from
schema work, and the tuning story would be dishonest.
"""

# ---------------------------------------------------------------------------
# Q1 -- tenant dashboard. The landing page of the customer-facing analytics
# product: for one account over a 7-day window, daily volume, resolution and
# escalation rates, and agent latency percentiles.
# ---------------------------------------------------------------------------

Q1_NAIVE = """
SELECT
    toDate(parseDateTimeBestEffort(event_ts))              AS day,
    count()                                                AS events,
    countIf(event_type = 'conversation_started')           AS conversations,
    countIf(event_type = 'resolution')                     AS resolutions,
    countIf(event_type = 'escalation')                     AS escalations,
    round(countIf(event_type = 'escalation')
          / nullIf(countIf(event_type = 'conversation_started'), 0), 4)
                                                           AS escalation_rate,
    round(quantileTDigestIf(0.95)(
        JSONExtractUInt(payload, 'latency_ms'),
        event_type = 'agent_response'))                    AS p95_latency_ms
FROM {table}
WHERE account_id = {account_id:String}
  AND parseDateTimeBestEffort(event_ts) >= {start:DateTime64}
  AND parseDateTimeBestEffort(event_ts) <  {end:DateTime64}
GROUP BY day
ORDER BY day
"""

Q1_TYPED = """
SELECT
    toDate(event_ts)                                       AS day,
    count()                                                AS events,
    countIf(event_type = 'conversation_started')           AS conversations,
    countIf(event_type = 'resolution')                     AS resolutions,
    countIf(event_type = 'escalation')                     AS escalations,
    round(countIf(event_type = 'escalation')
          / nullIf(countIf(event_type = 'conversation_started'), 0), 4)
                                                           AS escalation_rate,
    round(quantileTDigestIf(0.95)(
        latency_ms, event_type = 'agent_response'))        AS p95_latency_ms
FROM {table}
WHERE account_id = {account_id:String}
  AND event_ts >= {start:DateTime64}
  AND event_ts <  {end:DateTime64}
GROUP BY day
ORDER BY day
"""

Q1_MV = """
SELECT
    day,
    sum(events)                                            AS events,
    sum(conversations)                                     AS conversations,
    sum(resolutions)                                       AS resolutions,
    sum(escalations)                                       AS escalations,
    round(escalations / nullIf(conversations, 0), 4)       AS escalation_rate,
    round(quantilesTDigestMerge(0.5, 0.95, 0.99)(latency_state)[2])
                                                           AS p95_latency_ms
FROM conversation_daily
WHERE account_id = {account_id:String}
  AND day >= toDate({start:DateTime64})
  AND day <  toDate({end:DateTime64})
GROUP BY day
ORDER BY day
"""

# ---------------------------------------------------------------------------
# Q2 -- conversation drill-down. A support engineer opens one conversation.
# The filter column is a UUID with tens of millions of distinct values, so
# neither the sorting key nor partitioning can help: this is the query that
# justifies a bloom filter skipping index (or a re-sorted projection).
# ---------------------------------------------------------------------------

Q2_NAIVE = """
SELECT
    parseDateTime64BestEffort(event_ts, 3)                 AS event_ts,
    event_type,
    model,
    channel,
    intent,
    JSONExtractUInt(payload, 'latency_ms')                 AS latency_ms
FROM {table}
WHERE conversation_id = {conversation_id:String}
ORDER BY event_ts, event_type
"""

Q2_TYPED = """
SELECT
    event_ts,
    event_type,
    model,
    channel,
    intent,
    latency_ms
FROM {table}
WHERE conversation_id = {conversation_id:UUID}
ORDER BY event_ts, event_type
"""

# ---------------------------------------------------------------------------
# Q3 -- platform-wide health. The internal ops view: every tenant, one week.
# There is no account filter, so the leading sorting-key column buys nothing
# and partition pruning on the time range is the only structural win. This is
# the query that isolates the value of PARTITION BY.
# ---------------------------------------------------------------------------

Q3_NAIVE = """
SELECT
    toDate(parseDateTimeBestEffort(event_ts))              AS day,
    countIf(event_type = 'conversation_started')           AS conversations,
    countIf(event_type = 'escalation')                     AS escalations,
    round(quantileTDigestIf(0.99)(
        JSONExtractUInt(payload, 'latency_ms'),
        event_type = 'agent_response'))                    AS p99_latency_ms
FROM {table}
WHERE parseDateTimeBestEffort(event_ts) >= {start:DateTime64}
  AND parseDateTimeBestEffort(event_ts) <  {end:DateTime64}
GROUP BY day
ORDER BY day
"""

Q3_TYPED = """
SELECT
    toDate(event_ts)                                       AS day,
    countIf(event_type = 'conversation_started')           AS conversations,
    countIf(event_type = 'escalation')                     AS escalations,
    round(quantileTDigestIf(0.99)(
        latency_ms, event_type = 'agent_response'))        AS p99_latency_ms
FROM {table}
WHERE event_ts >= {start:DateTime64}
  AND event_ts <  {end:DateTime64}
GROUP BY day
ORDER BY day
"""

Q3_MV = """
SELECT
    day,
    sum(conversations)                                     AS conversations,
    sum(escalations)                                       AS escalations,
    round(quantilesTDigestMerge(0.5, 0.95, 0.99)(latency_state)[3])
                                                           AS p99_latency_ms
FROM platform_daily
WHERE day >= toDate({start:DateTime64})
  AND day <  toDate({end:DateTime64})
GROUP BY day
ORDER BY day
"""

# ---------------------------------------------------------------------------
# Q4 -- slow agent responses, sliced by model and channel. Latency regression
# triage: "which model/channel combination is blowing the SLO this week?".
# The filter is on a numeric column that is not in the sorting key, which is
# the case a minmax skipping index is supposed to serve -- and, measurably,
# does not. See the negative-result section of the README.
# ---------------------------------------------------------------------------

Q4_NAIVE = """
SELECT
    model,
    channel,
    count()                                                AS slow_responses,
    round(quantileTDigest(0.99)(
        JSONExtractUInt(payload, 'latency_ms')))           AS p99_latency_ms
FROM {table}
WHERE account_id = {account_id:String}
  AND parseDateTimeBestEffort(event_ts) >= {start:DateTime64}
  AND parseDateTimeBestEffort(event_ts) <  {end:DateTime64}
  AND event_type = 'agent_response'
  AND JSONExtractUInt(payload, 'latency_ms') > 5000
GROUP BY model, channel
ORDER BY slow_responses DESC, model, channel
"""

Q4_TYPED = """
SELECT
    model,
    channel,
    count()                                                AS slow_responses,
    round(quantileTDigest(0.99)(latency_ms))               AS p99_latency_ms
FROM {table}
WHERE account_id = {account_id:String}
  AND event_ts >= {start:DateTime64}
  AND event_ts <  {end:DateTime64}
  AND event_type = 'agent_response'
  AND latency_ms > 5000
GROUP BY model, channel
ORDER BY slow_responses DESC, model, channel
"""

#: query key -> {variant -> SQL template}. ``{table}`` is filled per stage.
BENCH_QUERIES = {
    "q1_tenant_dashboard": {
        "naive": Q1_NAIVE,
        "typed": Q1_TYPED,
        "mv": Q1_MV,
    },
    "q2_conversation_lookup": {
        "naive": Q2_NAIVE,
        "typed": Q2_TYPED,
    },
    "q3_platform_wide": {
        "naive": Q3_NAIVE,
        "typed": Q3_TYPED,
        "mv": Q3_MV,
    },
    "q4_slow_responses": {
        "naive": Q4_NAIVE,
        "typed": Q4_TYPED,
    },
}

QUERY_TITLES = {
    "q1_tenant_dashboard": "Tenant dashboard (1 account, 7 days, daily rollup)",
    "q2_conversation_lookup": "Conversation drill-down (single UUID needle)",
    "q3_platform_wide": "Platform-wide health (all tenants, 7 days)",
    "q4_slow_responses": "Slow responses by model/channel (latency > 5s)",
}


# ---------------------------------------------------------------------------
# Serving queries. These are what the FastAPI service runs; they read the
# materialized-view targets, never the raw events table, except for the
# drill-down which needs event-level rows.
# ---------------------------------------------------------------------------

API_CONVERSATION_SUMMARY = """
SELECT
    day,
    conversations,
    resolutions,
    escalations,
    agent_responses,
    round(resolutions / nullIf(conversations, 0), 4)       AS resolution_rate,
    round(escalations / nullIf(conversations, 0), 4)       AS escalation_rate,
    avg_sentiment,
    round(lat[1])                                          AS p50_latency_ms,
    round(lat[2])                                          AS p95_latency_ms,
    round(lat[3])                                          AS p99_latency_ms
FROM (
    SELECT
        day,
        sum(conversations)                                 AS conversations,
        sum(resolutions)                                   AS resolutions,
        sum(escalations)                                   AS escalations,
        sum(agent_responses)                               AS agent_responses,
        round(avgMerge(sentiment_state), 4)                AS avg_sentiment,
        quantilesTDigestMerge(0.5, 0.95, 0.99)(latency_state) AS lat
    FROM conversation_daily
    WHERE account_id = {account_id:String}
      AND day >= {start:Date}
      AND day <  {end:Date}
    GROUP BY day
)
ORDER BY day
"""

API_AGENT_LATENCY = """
SELECT
    model,
    channel,
    responses,
    slow_responses,
    round(slow_responses / nullIf(responses, 0), 4)        AS slow_rate,
    total_tokens,
    round(lat[1])                                          AS p50_latency_ms,
    round(lat[2])                                          AS p95_latency_ms,
    round(lat[3])                                          AS p99_latency_ms
FROM (
    SELECT
        model,
        channel,
        sum(responses)                                     AS responses,
        sum(slow_responses)                                AS slow_responses,
        sum(prompt_tokens) + sum(completion_tokens)        AS total_tokens,
        quantilesTDigestMerge(0.5, 0.95, 0.99)(latency_state) AS lat
    FROM agent_hourly
    WHERE account_id = {account_id:String}
      AND hour >= {start:DateTime64}
      AND hour <  {end:DateTime64}
    GROUP BY model, channel
)
ORDER BY responses DESC, model, channel
"""

API_HOURLY_VOLUME = """
SELECT
    hour,
    sum(responses)                                         AS agent_responses,
    round(quantilesTDigestMerge(0.5, 0.95, 0.99)(latency_state)[2])
                                                           AS p95_latency_ms
FROM agent_hourly
WHERE account_id = {account_id:String}
  AND hour >= {start:DateTime64}
  AND hour <  {end:DateTime64}
GROUP BY hour
ORDER BY hour
"""

API_CONVERSATION_DETAIL = """
SELECT
    event_ts,
    event_type,
    model,
    channel,
    intent,
    latency_ms,
    prompt_tokens,
    completion_tokens,
    sentiment,
    escalation_reason
FROM {events_table}
WHERE conversation_id = {conversation_id:UUID}
ORDER BY event_ts, event_type
LIMIT 500
"""

API_TOP_INTENTS = """
SELECT
    intent,
    count()                                                AS events,
    countIf(event_type = 'escalation')                     AS escalations
FROM {events_table}
WHERE account_id = {account_id:String}
  AND event_ts >= {start:DateTime64}
  AND event_ts <  {end:DateTime64}
GROUP BY intent
ORDER BY events DESC, intent
LIMIT 20
"""
