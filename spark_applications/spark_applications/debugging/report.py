"""The shared shape of a debugging case.

Every case in this package answers the same five questions in the same order,
so they read as a series rather than seven unrelated scripts:

    symptom     what you actually saw — the error text, or the observation
    evidence    the specific plan lines that identify the cause
    cause       what is really going on
    resolution  what to change
    notes       when the fix does not apply, and what to check next

:class:`Diagnosis` is that shape. Cases return one; :func:`render` prints it.
"""

from dataclasses import dataclass, field

from spark_applications.debugging.explain_tools import (
    diff_plans,
    summarize_plan,
)

_WIDTH = 78


def _rule(char: str = "-") -> str:
    return char * _WIDTH


def _heading(text: str) -> str:
    return f"\n{_rule('=')}\n{text}\n{_rule('=')}"


def _section(text: str) -> str:
    return f"\n{text}\n{_rule()}"


def _indent(text: str, prefix: str = "  ") -> str:
    return "\n".join(prefix + line for line in text.splitlines())


@dataclass
class Evidence:
    """One observation from a plan: what to look for, and what was found."""

    look_for: str
    broken: str
    fixed: str


@dataclass
class Diagnosis:
    """A complete worked debugging case."""

    case_id: str
    title: str
    symptom: str
    cause: str
    resolution: str
    broken_plan: str = ""
    fixed_plan: str = ""
    evidence: list[Evidence] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    metrics: dict[str, str] = field(default_factory=dict)

    def render(self, show_plans: bool = True) -> str:
        """Format the whole case for terminal output."""
        parts = [_heading(f"{self.case_id}  {self.title}")]

        parts.append(_section("SYMPTOM"))
        parts.append(_indent(self.symptom.strip()))

        if show_plans and self.broken_plan:
            parts.append(_section("PLAN — broken"))
            parts.append(summarize_plan(self.broken_plan).render())

        if show_plans and self.fixed_plan:
            parts.append(_section("PLAN — fixed"))
            parts.append(summarize_plan(self.fixed_plan).render())

        if self.evidence:
            parts.append(_section("EVIDENCE — what to look for in the plan"))
            for item in self.evidence:
                parts.append(f"  {item.look_for}")
                parts.append(f"      broken : {item.broken}")
                parts.append(f"      fixed  : {item.fixed}")

        if self.metrics:
            parts.append(_section("MEASURED"))
            width = max(len(name) for name in self.metrics)
            for name, value in self.metrics.items():
                parts.append(f"  {name:<{width}} : {value}")

        parts.append(_section("CAUSE"))
        parts.append(_indent(self.cause.strip()))

        parts.append(_section("RESOLUTION"))
        parts.append(_indent(self.resolution.strip()))

        if self.notes:
            parts.append(_section("NOTES"))
            for note in self.notes:
                parts.append(_indent(f"- {note.strip()}"))

        return "\n".join(parts) + "\n"

    def plan_diff(self) -> str:
        """Unified diff of the broken and fixed plans."""
        return diff_plans(self.broken_plan, self.fixed_plan)
