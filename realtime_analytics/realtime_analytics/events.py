"""AI agent conversation event model.

The domain is a conversational-AI support platform: an end customer talks to
an AI agent over some channel (chat, voice, email, sms, api) and the
conversation either resolves or escalates to a human.

Every interaction emits an event:

======================  ====================================================
event_type              meaning
======================  ====================================================
conversation_started    a customer opened a new conversation
message_sent            the customer sent a message / utterance
agent_response          the AI agent replied (carries model + latency + tokens)
resolution              the agent closed the conversation successfully
escalation              the conversation was handed off to a human agent
======================  ====================================================

``account_id`` is the tenant (the brand deploying the agent). Nearly every
analytical query filters on it, which is why it leads the sorting key -- see
the schema discussion in README.md.
"""
import json
import random
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta

EVENT_TYPES = [
    "conversation_started",
    "message_sent",
    "agent_response",
    "resolution",
    "escalation",
]

CHANNELS = ["chat", "voice", "email", "sms", "api"]

LOCALES = ["en-US", "en-GB", "es-MX", "pt-BR", "fr-FR", "de-DE", "ja-JP"]

MODELS = ["agent-lg-v3", "agent-lg-v2", "agent-sm-v3", "agent-voice-v1"]

# Deliberately high cardinality: intent is the classic "needle" filter that
# motivates a skipping index rather than a sorting-key column.
INTENTS = [
    "order_status",
    "refund_request",
    "billing_dispute",
    "password_reset",
    "cancel_subscription",
    "shipping_delay",
    "product_question",
    "account_closure",
    "fraud_report",
    "plan_upgrade",
    "technical_issue",
    "store_hours",
]

ESCALATION_REASONS = [
    "",
    "low_confidence",
    "customer_requested_human",
    "policy_restricted",
    "repeated_failure",
    "negative_sentiment",
]


@dataclass
class ConversationEvent:
    """A single event on an AI agent conversation."""

    event_id: str
    conversation_id: str
    account_id: str
    user_id: str
    agent_id: str
    event_type: str
    channel: str
    locale: str
    model: str
    intent: str
    event_ts: str
    ingest_ts: str
    latency_ms: int
    prompt_tokens: int
    completion_tokens: int
    sentiment: float
    resolved: int
    escalation_reason: str = ""
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    def to_row(self) -> list:
        """Column-ordered row matching ``COLUMNS`` for a ClickHouse insert."""
        return [getattr(self, name) for name in COLUMNS]


COLUMNS = [
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


def _agent_latency_ms(rng: random.Random) -> int:
    """Agent response latency with a realistic long tail.

    ~92% of responses are fast, ~7% are slow, ~1% are pathological (a tool
    call or model retry). The tail is what makes p95/p99 interesting and is
    the reason the benchmark reports quantiles rather than averages.
    """
    roll = rng.random()
    if roll < 0.92:
        return rng.randint(180, 1200)
    if roll < 0.99:
        return rng.randint(1200, 4000)
    return rng.randint(4000, 15000)


def generate_conversation(
    rng: random.Random,
    account_id: str,
    start_ts: datetime,
    escalation_rate: float = 0.18,
) -> list[ConversationEvent]:
    """Generate one full conversation as an ordered list of events.

    The sequence is always ``conversation_started`` -> alternating
    ``message_sent`` / ``agent_response`` turns -> exactly one terminal
    ``resolution`` or ``escalation``. Downstream aggregations rely on that
    invariant (one terminal event per conversation), so conversation counts
    and resolution rates are directly comparable.
    """
    conversation_id = str(uuid.UUID(int=rng.getrandbits(128)))
    user_id = str(uuid.UUID(int=rng.getrandbits(128)))
    channel = rng.choice(CHANNELS)
    locale = rng.choice(LOCALES)
    model = "agent-voice-v1" if channel == "voice" else rng.choice(MODELS[:3])
    intent = rng.choice(INTENTS)
    agent_id = f"agent_{rng.randint(1, 40):03d}"

    escalated = rng.random() < escalation_rate
    turns = rng.randint(2, 6)
    events: list[ConversationEvent] = []
    ts = start_ts
    sentiment = rng.uniform(-0.2, 0.6)

    def emit(event_type: str, latency_ms: int = 0, **overrides) -> None:
        nonlocal ts
        payload = {
            "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "conversation_id": conversation_id,
            "account_id": account_id,
            "user_id": user_id,
            "agent_id": agent_id,
            "event_type": event_type,
            "channel": channel,
            "locale": locale,
            "model": model,
            "intent": intent,
            "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            # Ingest lag is what the Flink watermark has to absorb.
            "ingest_ts": (
                ts + timedelta(milliseconds=rng.randint(20, 2500))
            ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "latency_ms": latency_ms,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "sentiment": round(sentiment, 3),
            "resolved": 0,
            "escalation_reason": "",
        }
        payload.update(overrides)
        events.append(ConversationEvent(**payload))
        ts = ts + timedelta(milliseconds=rng.randint(400, 9000))

    emit("conversation_started")
    for _ in range(turns):
        emit("message_sent")
        latency = _agent_latency_ms(rng)
        emit(
            "agent_response",
            latency_ms=latency,
            prompt_tokens=rng.randint(400, 3000),
            completion_tokens=rng.randint(30, 600),
        )
        # Each unhelpful turn drags sentiment down; escalated conversations
        # end unhappy, which makes the sentiment column worth aggregating.
        sentiment -= rng.uniform(0.0, 0.15) if escalated else -rng.uniform(0.0, 0.1)

    if escalated:
        emit("escalation", escalation_reason=rng.choice(ESCALATION_REASONS[1:]))
    else:
        emit("resolution", resolved=1)
    return events


def generate_events(
    count: int,
    seed: int = 42,
    accounts: int = 25,
    start_ts: datetime | None = None,
    window_hours: int = 24,
):
    """Yield up to ``count`` events across ``accounts`` tenants.

    Account traffic is deliberately skewed (a handful of large enterprise
    tenants dominate) because that skew is what makes tenant-leading sorting
    keys and per-tenant partition pruning worth measuring.
    """
    rng = random.Random(seed)
    start_ts = start_ts or datetime.now() - timedelta(hours=window_hours)
    account_ids = [f"acct_{i:04d}" for i in range(accounts)]
    # Zipf-ish weights: acct_0000 gets ~ 25x the traffic of the smallest.
    weights = [1.0 / (i + 1) for i in range(accounts)]

    emitted = 0
    while emitted < count:
        account_id = rng.choices(account_ids, weights=weights, k=1)[0]
        offset = timedelta(seconds=rng.randint(0, window_hours * 3600))
        # Enterprise tenants run hotter agents and escalate less.
        idx = account_ids.index(account_id)
        escalation_rate = 0.10 + 0.02 * (idx % 8)
        for event in generate_conversation(
            rng, account_id, start_ts + offset, escalation_rate
        ):
            if emitted >= count:
                return
            emitted += 1
            yield event
