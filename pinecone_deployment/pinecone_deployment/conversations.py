"""Synthetic conversations to embed and retrieve.

The platform's events (``realtime_analytics``) carry intent, channel, locale,
sentiment, escalation and resolution for each conversation but no text. To
give the vector index something worth searching, this module renders a short
transcript and a resolution note from those fields with templated utterance
banks per intent, deterministically from a seed, so every run and test sees
the same corpus and the labelled ground truth (the intent) is known.

Vocabulary matches ``realtime_analytics/events.py`` so ids and metadata line
up if the two are ever joined.
"""

import random
import uuid
from dataclasses import asdict, dataclass

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
CHANNELS = ["chat", "voice", "email", "sms", "api"]
LOCALES = ["en-US", "en-GB", "es-MX", "pt-BR", "fr-FR", "de-DE", "ja-JP"]
ESCALATION_REASONS = [
    "low_confidence",
    "customer_requested_human",
    "policy_restricted",
    "repeated_failure",
    "negative_sentiment",
]

# Two or three phrasings per intent so nearest neighbours are not exact
# duplicates, plus a shared filler bank that adds noise a good embedding
# should see through and a hashing embedder partly will not.
UTTERANCES = {
    "order_status": [
        "Where is my order? It was supposed to arrive yesterday.",
        "Can you tell me the status of order {ref}? Tracking hasn't updated.",
        "I placed an order last week and still have no shipping update.",
    ],
    "refund_request": [
        "I want a refund for order {ref}, the item arrived damaged.",
        "How do I get my money back? The product is not what I ordered.",
        "Please refund my last purchase, I returned it two weeks ago.",
    ],
    "billing_dispute": [
        "I was charged twice this month, please explain this invoice.",
        "There is a charge on my card I don't recognise from you.",
        "My bill is higher than my plan says it should be.",
    ],
    "password_reset": [
        "I can't log in, the reset email never arrives.",
        "How do I reset my password? The link in the email is expired.",
        "Locked out of my account after too many attempts.",
    ],
    "cancel_subscription": [
        "I'd like to cancel my subscription before the next renewal.",
        "Please stop billing me, I no longer use the service.",
        "How do I cancel? I can't find the option in settings.",
    ],
    "shipping_delay": [
        "My delivery is late by a week, what is going on?",
        "The carrier says the package is delayed with no new date.",
        "Shipping estimate moved twice already, I need it by Friday.",
    ],
    "product_question": [
        "Does this model work with the older charging dock?",
        "What is the difference between the standard and pro version?",
        "Is the {ref} compatible with my existing setup?",
    ],
    "account_closure": [
        "Please delete my account and all my data.",
        "I want to close my account permanently.",
        "How do I remove my profile and stop all emails?",
    ],
    "fraud_report": [
        "Someone used my account to place orders I didn't make.",
        "I think my card details were stolen through your site.",
        "There are logins from a country I've never been to.",
    ],
    "plan_upgrade": [
        "I want to move to the higher tier plan, what does it include?",
        "Can I upgrade mid-cycle and is it prorated?",
        "Which plan gives me more seats for my team?",
    ],
    "technical_issue": [
        "The app crashes every time I open the reports tab.",
        "Uploads fail with an error after about ten seconds.",
        "The integration stopped syncing since last night.",
    ],
    "store_hours": [
        "What time does the downtown store open on Sunday?",
        "Are you open on public holidays?",
        "Closing time for the airport location today?",
    ],
}

RESOLUTIONS = {
    "order_status": (
        "Located the shipment, shared carrier tracking and a new ETA."
    ),
    "refund_request": (
        "Approved the refund to the original payment method, 5-7 days."
    ),
    "billing_dispute": (
        "Reversed the duplicate charge and sent a corrected invoice."
    ),
    "password_reset": (
        "Verified identity and issued a fresh reset link; login "
        "confirmed."
    ),
    "cancel_subscription": (
        "Cancelled at period end, confirmation email sent."
    ),
    "shipping_delay": (
        "Confirmed carrier delay, offered reshipment or refund."
    ),
    "product_question": (
        "Confirmed compatibility from the spec sheet and linked it."
    ),
    "account_closure": (
        "Scheduled deletion after the 30-day retention window."
    ),
    "fraud_report": (
        "Locked the account, reversed the orders, forced re- "
        "authentication."
    ),
    "plan_upgrade": (
        "Upgraded with proration; new limits active immediately."
    ),
    "technical_issue": (
        "Reproduced the crash, linked the known issue and workaround."
    ),
    "store_hours": (
        "Shared the location's hours and holiday schedule."
    ),
}

FILLERS = [
    "Thanks in advance.",
    "This is urgent.",
    "I've been a customer for years.",
    "Please help.",
    "I already tried the FAQ.",
    "",
]

AGENT_OPENERS = [
    "Sorry to hear that. Let me take a look.",
    "I can help with that. One moment.",
    "Thanks for reaching out, checking now.",
]


@dataclass
class Conversation:
    conversation_id: str
    account_id: str
    intent: str
    channel: str
    locale: str
    sentiment: float
    escalated: bool
    escalation_reason: str
    transcript: str
    resolution: str

    @property
    def vector_id(self) -> str:
        """Deterministic id: account + conversation. Re-embedding the same
        conversation upserts in place instead of duplicating it."""
        return f"{self.account_id}:{self.conversation_id}"

    def metadata(self) -> dict:
        """Metadata stored with the vector. Kept small and typed: Pinecone
        indexes every metadata field by default and bills storage for it,
        and filters only work on scalars / lists of strings."""
        return {
            "account_id": self.account_id,
            "intent": self.intent,
            "channel": self.channel,
            "locale": self.locale,
            "sentiment": round(self.sentiment, 3),
            "escalated": self.escalated,
            "escalation_reason": self.escalation_reason,
            "resolution": self.resolution,
        }

    def to_dict(self) -> dict:
        return asdict(self)


def render_transcript(rng: random.Random, intent: str) -> str:
    ref = f"#{rng.randint(10_000, 99_999)}"
    user = rng.choice(UTTERANCES[intent]).format(ref=ref)
    filler = rng.choice(FILLERS)
    agent = rng.choice(AGENT_OPENERS)
    return " ".join(
        part
        for part in (
            f"Customer: {user}",
            filler and f"Customer: {filler}",
            f"Agent: {agent}",
        )
        if part
    )


def generate(
    count: int = 1_000,
    accounts: int = 20,
    seed: int = 7,
    escalation_rate: float = 0.18,
) -> list[Conversation]:
    """Deterministic corpus: ``count`` conversations, ``accounts`` tenants."""
    rng = random.Random(seed)
    account_ids = [f"acct_{i:04d}" for i in range(accounts)]
    conversations = []
    for _ in range(count):
        intent = rng.choice(INTENTS)
        escalated = rng.random() < escalation_rate
        conversations.append(
            Conversation(
                conversation_id=str(uuid.UUID(int=rng.getrandbits(128))),
                account_id=rng.choice(account_ids),
                intent=intent,
                channel=rng.choice(CHANNELS),
                locale=rng.choice(LOCALES),
                sentiment=(
                    rng.uniform(-0.8, 0.2)
                    if escalated
                    else rng.uniform(-0.2, 0.6)
                ),
                escalated=escalated,
                escalation_reason=(
                    rng.choice(ESCALATION_REASONS) if escalated else ""
                ),
                transcript=render_transcript(rng, intent),
                resolution=RESOLUTIONS[intent],
            )
        )
    return conversations
