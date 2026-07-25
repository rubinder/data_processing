"""Deterministic, in-engine generation of AI agent conversation events.

The benchmark needs tens of millions of rows. Producing them in Python and
shipping them over the wire would make the *loader* the bottleneck and would
add minutes to every run, so the rows are generated inside ClickHouse with
``INSERT ... SELECT FROM numbers()``.

Everything is derived from ``number`` through ``cityHash64``, so:

* the naive table and every optimized table contain **byte-identical logical
  data** -- any measured difference is caused by schema and layout, never by
  different data;
* a run is reproducible on any machine, which matters when the README quotes
  before/after numbers.

Row layout: 8 consecutive ``number`` values form one conversation
(started -> 3 x (message_sent, agent_response) -> resolution|escalation).
"""

EVENTS_PER_CONVERSATION = 8

# All generated data sits in a fixed window so quoted benchmark queries stay
# valid regardless of when the suite is run.
ANCHOR_TS = "2026-04-01 00:00:00"
WINDOW_DAYS = 90
ACCOUNTS = 200

ESCALATION_REASONS = (
    "low_confidence",
    "customer_requested_human",
    "policy_restricted",
    "repeated_failure",
    "negative_sentiment",
)


def _uuid_expr(hash_expr: str, salt: str) -> str:
    """Render a deterministic, well-formed UUID string from two hashes.

    ``toUUID`` needs canonical 8-4-4-4-12 hex, so two 64-bit hashes are hex
    encoded into 32 characters and the dashes are spliced back in. ``hex``
    strips leading zeros, hence the explicit pad to 16 characters per half.

    ``salt`` keeps ID families disjoint: without it ``event_id`` and
    ``conversation_id`` would collide wherever ``number == conv``.
    """
    hex32 = (
        f"concat(leftPad(lower(hex(cityHash64({hash_expr}, '{salt}hi'))), 16, '0'), "
        f"leftPad(lower(hex(cityHash64({hash_expr}, '{salt}lo'))), 16, '0'))"
    )
    return (
        "toUUID(concat("
        f"substring({hex32}, 1, 8), '-', "
        f"substring({hex32}, 9, 4), '-', "
        f"substring({hex32}, 13, 4), '-', "
        f"substring({hex32}, 17, 4), '-', "
        f"substring({hex32}, 21, 12)))"
    )


def _reasons_array() -> str:
    return "[" + ", ".join(f"'{r}'" for r in ESCALATION_REASONS) + "]"


def base_select(count: int, offset: int = 0) -> str:
    """The shared expression block every schema variant is populated from.

    Returned as a sub-select exposing fully typed columns. ClickHouse resolves
    aliases within a SELECT list, so each expression can build on the previous
    one instead of repeating the hash arithmetic.
    """
    span_seconds = WINDOW_DAYS * 24 * 3600
    return f"""
SELECT
    intDiv(number, {EVENTS_PER_CONVERSATION})            AS conv,
    toUInt8(number % {EVENTS_PER_CONVERSATION})          AS pos,
    cityHash64(conv, 'conv')                             AS ch,
    cityHash64(number, 'evt')                            AS eh,
    -- Tenant traffic is heavily skewed: the exponent biases most rows onto a
    -- small set of enterprise accounts, mirroring a real B2B footprint.
    toUInt16(floor(pow((ch % 1000000) / 1000000.0, 2.2) * {ACCOUNTS}))
                                                         AS acct_idx,
    concat('acct_', leftPad(toString(acct_idx), 4, '0')) AS account_id,
    toUInt8(10 + (acct_idx % 8) * 2)                     AS esc_pct,
    multiIf(
        pos = 0, 'conversation_started',
        pos = 7, if(ch % 100 < esc_pct, 'escalation', 'resolution'),
        pos % 2 = 1, 'message_sent',
        'agent_response'
    )                                                    AS event_type,
    toDateTime('{ANCHOR_TS}') + (ch % {span_seconds}) + (pos * 7)
                                                         AS base_ts,
    toInt64(toUnixTimestamp(base_ts)) * 1000 + (eh % 1000)
                                                         AS event_ms,
    fromUnixTimestamp64Milli(event_ms)                   AS event_ts,
    -- Ingest lag: 20ms .. 2.5s, which is what the Flink watermark absorbs.
    fromUnixTimestamp64Milli(event_ms + 20 + (eh % 2480)) AS ingest_ts,
    {_uuid_expr('number', 'evt')}                        AS event_id,
    {_uuid_expr('conv', 'cnv')}                          AS conversation_id,
    concat('u_', toString(cityHash64(acct_idx, ch % 50000)))
                                                         AS user_id,
    concat('agent_', leftPad(toString(ch % 40), 3, '0')) AS agent_id,
    arrayElement(
        ['chat', 'voice', 'email', 'sms', 'api'], toUInt8(ch % 5) + 1
    )                                                    AS channel,
    arrayElement(
        ['en-US', 'en-GB', 'es-MX', 'pt-BR', 'fr-FR', 'de-DE', 'ja-JP'],
        toUInt8(ch % 7) + 1
    )                                                    AS locale,
    if(channel = 'voice', 'agent-voice-v1', arrayElement(
        ['agent-lg-v3', 'agent-lg-v2', 'agent-sm-v3'], toUInt8(ch % 3) + 1
    ))                                                   AS model,
    arrayElement(
        ['order_status', 'refund_request', 'billing_dispute',
         'password_reset', 'cancel_subscription', 'shipping_delay',
         'product_question', 'account_closure', 'fraud_report',
         'plan_upgrade', 'technical_issue', 'store_hours'],
        toUInt8(ch % 12) + 1
    )                                                    AS intent,
    -- Long-tailed agent latency: ~92% fast, ~7% slow, ~1% pathological.
    if(event_type = 'agent_response', toUInt32(multiIf(
        eh % 100 < 92, 180 + (eh % 1020),
        eh % 100 < 99, 1200 + (eh % 2800),
        4000 + (eh % 11000)
    )), toUInt32(0))                                     AS latency_ms,
    if(event_type = 'agent_response', toUInt32(400 + (eh % 2600)), toUInt32(0))
                                                         AS prompt_tokens,
    if(event_type = 'agent_response', toUInt32(30 + (eh % 570)), toUInt32(0))
                                                         AS completion_tokens,
    toFloat32(round(-1 + (eh % 2000) / 1000.0, 3))       AS sentiment,
    toUInt8(event_type = 'resolution')                   AS resolved,
    if(event_type = 'escalation',
       arrayElement({_reasons_array()}, toUInt8(eh % 5) + 1), '')
                                                         AS escalation_reason
FROM numbers({offset}, {count})
"""


#: Columns shared by every *typed* variant, in physical order.
TYPED_COLUMNS = [
    "event_id",
    "conversation_id",
    "account_id",
    "user_id",
    "agent_id",
    "event_type",
    "channel",
    "locale",
    "model",
    "intent",
    "event_ts",
    "ingest_ts",
    "latency_ms",
    "prompt_tokens",
    "completion_tokens",
    "sentiment",
    "resolved",
    "escalation_reason",
]


def insert_typed(table: str, count: int, offset: int = 0) -> str:
    """INSERT statement populating a typed (optimized) events table."""
    cols = ", ".join(TYPED_COLUMNS)
    projection = ",\n    ".join(TYPED_COLUMNS)
    return f"""
INSERT INTO {table} ({cols})
SELECT
    {projection}
FROM ({base_select(count, offset)})
"""


def insert_naive(table: str, count: int, offset: int = 0) -> str:
    """INSERT statement populating the naive, all-String events table.

    Timestamps become ISO-8601 text and the numeric measures are buried in a
    JSON blob -- the shape you get when a service dumps its event payload
    straight into a table without thinking about the read path.
    """
    return f"""
INSERT INTO {table} (
    event_id, conversation_id, account_id, user_id, agent_id, event_type,
    channel, locale, model, intent, event_ts, ingest_ts, payload
)
SELECT
    toString(event_id),
    toString(conversation_id),
    account_id,
    user_id,
    agent_id,
    event_type,
    channel,
    locale,
    model,
    intent,
    -- Deliberately timezone-naive text, matching the typed tables' local
    -- DateTime64. An ISO 'Z' suffix here would make parseDateTimeBestEffort
    -- shift into the session timezone and silently move rows across day
    -- boundaries, so the stages would no longer be comparable.
    concat(formatDateTime(event_ts, '%Y-%m-%dT%H:%i:%S'), '.',
           leftPad(toString(toUnixTimestamp64Milli(event_ts) % 1000), 3, '0'))
                                                         AS event_ts,
    concat(formatDateTime(ingest_ts, '%Y-%m-%dT%H:%i:%S'), '.',
           leftPad(toString(toUnixTimestamp64Milli(ingest_ts) % 1000), 3, '0'))
                                                         AS ingest_ts,
    concat(
        '{{"latency_ms":', toString(latency_ms),
        ',"prompt_tokens":', toString(prompt_tokens),
        ',"completion_tokens":', toString(completion_tokens),
        ',"sentiment":', toString(sentiment),
        ',"resolved":', toString(resolved),
        ',"escalation_reason":"', escalation_reason, '"}}'
    )                                                    AS payload
FROM ({base_select(count, offset)})
"""
