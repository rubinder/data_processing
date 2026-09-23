"""Per-feed declarative configuration: one YAML per feed.

Everything about a feed except its Iceberg DDL -- which layers it has, which
contract each layer is held to, what the agent watches, which monitors run
and what arrival SLA applies -- is declared in ``feeds/<name>.yaml``. Adding
a feed is one new file and zero Python edits.

Validation fails loudly and names the offending key. A config error that
degrades to a default is a check that reports green while measuring nothing.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from ops_agent import config, tables
from ops_agent.monitors import KINDS, MonitorDef

VALID_INGEST_MODES = frozenset({"batch", "stream"})
VALID_CALENDARS = frozenset({"daily"})
VALID_FEED_TYPES = frozenset({"fact", "event", "dimension"})


class FeedConfigError(Exception):
    """A feed config that cannot be trusted. Never degrades to a default."""


@dataclass(frozen=True)
class LayerDef:
    layer: str
    table: str
    contract: str | None = None
    freshness_column: str | None = None
    agent_watch: bool = False
    typed: bool = False


@dataclass(frozen=True)
class ArrivalDef:
    table: str
    column: str
    calendar: str
    max_lag_periods: int
    min_rows_per_period: int | None


@dataclass(frozen=True)
class FeedDef:
    name: str
    domain: str
    feed_type: str
    owner: str
    ingest_mode: str
    layers: dict[str, LayerDef]
    monitors: tuple[MonitorDef, ...]
    arrival: tuple[ArrivalDef, ...]

    @property
    def watched_layers(self) -> tuple[LayerDef, ...]:
        return tuple(a for a in self.layers.values() if a.agent_watch)

    @property
    def typed_layers(self) -> tuple[LayerDef, ...]:
        return tuple(a for a in self.layers.values() if a.typed)


def _require(raw: dict, key: str, where: str):
    if key not in raw:
        raise FeedConfigError(f"{where}: missing required key '{key}'")
    return raw[key]


def table_def_for(table: str) -> tables.TableDef:
    try:
        return tables.by_name(table)
    except KeyError:
        known = ", ".join(sorted(t.name for t in tables.ALL_TABLES))
        raise FeedConfigError(
            f"table '{table}' is not declared in tables.ALL_TABLES; known: {known}"
        ) from None


def _parse_layers(raw: dict, where: str, contract_dir: Path) -> dict[str, LayerDef]:
    layers_raw = _require(raw, "layers", where)
    if not isinstance(layers_raw, dict) or not layers_raw:
        raise FeedConfigError(f"{where}: 'layers' must be a non-empty mapping")

    layers: dict[str, LayerDef] = {}
    for name, body in layers_raw.items():
        spot = f"{where}: layers.{name}"
        body = body or {}
        table = _require(body, "table", spot)
        table_def_for(table)
        contract = body.get("contract")
        if contract and not (contract_dir / contract).exists():
            raise FeedConfigError(f"{spot}: contract '{contract}' does not exist in "
                                  f"{contract_dir}/")
        if body.get("typed") and not contract:
            raise FeedConfigError(
                f"{spot}: 'typed: true' needs a 'contract' -- column-type checks "
                "are generated per contract field, so without one the layer "
                "would silently produce no type checks at all")
        layers[name] = LayerDef(
            layer=name, table=table, contract=contract,
            freshness_column=body.get("freshness_column"),
            agent_watch=bool(body.get("agent_watch", False)),
            typed=bool(body.get("typed", False)))
    return layers


def _parse_monitors(raw: dict, layers: dict[str, LayerDef], where: str) -> tuple[MonitorDef, ...]:
    out: list[MonitorDef] = []
    for entry in raw.get("monitors") or []:
        name = _require(entry, "name", where)
        spot = f"{where}: monitor '{name}'"
        layer = _require(entry, "layer", spot)
        if layer not in layers:
            raise FeedConfigError(f"{spot}: 'layer: {layer}' is not declared in this "
                                  f"feed's layers ({', '.join(sorted(layers))})")
        kind = _require(entry, "kind", spot)
        if kind not in KINDS:
            raise FeedConfigError(f"{spot}: unknown kind '{kind}' (expected one of "
                                  f"{', '.join(sorted(KINDS))})")
        for alias, table in (entry.get("tables") or {}).items():
            try:
                table_def_for(table)
            except FeedConfigError as exc:
                raise FeedConfigError(f"{spot}: tables alias '{alias}' -> {exc}") from None
        out.append(MonitorDef(
            name=name, table=layers[layer].table, kind=kind,
            query=_require(entry, "query", spot), column=entry.get("column"),
            severity=entry.get("severity", "additive"),
            params=entry.get("params") or {},
            tables=entry.get("tables") or None,
            source=entry.get("source", "sql")))
    return tuple(out)


def _parse_arrival(raw: dict, layers: dict[str, LayerDef], where: str) -> tuple[ArrivalDef, ...]:
    out: list[ArrivalDef] = []
    for entry in raw.get("arrival") or []:
        layer = _require(entry, "layer", where)
        spot = f"{where}: arrival on layer '{layer}'"
        if layer not in layers:
            raise FeedConfigError(f"{spot}: not declared in this feed's layers "
                                  f"({', '.join(sorted(layers))})")
        calendar = _require(entry, "calendar", spot)
        if calendar not in VALID_CALENDARS:
            raise FeedConfigError(f"{spot}: unknown calendar '{calendar}' (expected one "
                                  f"of {', '.join(sorted(VALID_CALENDARS))})")
        floor = entry.get("min_rows_per_period")
        if floor is not None and int(floor) <= 1:
            # ``observed`` comes from a count(*) GROUP BY, so a period that
            # appears at all has at least one row and a floor of 1 is a
            # check that cannot fire, displayed as passing on every run.
            raise FeedConfigError(f"{spot}: min_rows_per_period must be null or > 1")
        out.append(ArrivalDef(
            table=layers[layer].table, column=_require(entry, "column", spot),
            calendar=calendar, max_lag_periods=int(_require(entry, "max_lag_periods", spot)),
            min_rows_per_period=int(floor) if floor is not None else None))
    return tuple(out)


def load_feed(path: Path, contract_dir: Path | None = None) -> FeedDef:
    where = f"feeds/{Path(path).name}"
    raw = yaml.safe_load(Path(path).read_text())
    if not isinstance(raw, dict):
        raise FeedConfigError(f"{where}: file is empty or not a mapping")
    name = _require(raw, "name", where)
    feed_type = raw.get("feed_type", "fact")
    if feed_type not in VALID_FEED_TYPES:
        raise FeedConfigError(f"{where}: unknown feed_type '{feed_type}' (expected one of "
                              f"{', '.join(sorted(VALID_FEED_TYPES))})")
    mode = (raw.get("ingest") or {}).get("mode", "batch")
    if mode not in VALID_INGEST_MODES:
        raise FeedConfigError(f"{where}: unknown ingest.mode '{mode}' (expected one of "
                              f"{', '.join(sorted(VALID_INGEST_MODES))})")
    layers = _parse_layers(raw, where, Path(contract_dir or config.CONTRACT_DIR))
    return FeedDef(
        name=name, domain=raw.get("domain", "unknown"), feed_type=feed_type,
        owner=raw.get("owner", "unknown"), ingest_mode=mode, layers=layers,
        monitors=_parse_monitors(raw, layers, where),
        arrival=_parse_arrival(raw, layers, where))


def load_feeds(directory: Path | None = None,
               contract_dir: Path | None = None) -> list[FeedDef]:
    """Every feed, sorted by name. Rejects duplicate monitor names globally.

    Monitor names are the join key between persisted history and a live
    definition; two feeds sharing one would interleave two measurements into
    one baseline series.
    """
    root = Path(directory or config.FEED_DIR)
    paths = sorted(root.glob("*.yaml"))
    if not paths:
        # No feeds is not "nothing to check", it is "the config is gone".
        raise FeedConfigError(
            f"no feed configs found in {root}/ -- every monitor, arrival SLA and "
            "agent watch is declared there, so an empty directory means zero "
            "checks would run and the platform would report clean while "
            "checking nothing")
    feeds = [load_feed(p, contract_dir) for p in paths]

    monitor_owner: dict[str, str] = {}
    typed_owner: dict[str, str] = {}
    watch_owner: dict[str, str] = {}
    for feed in feeds:
        for monitor in feed.monitors:
            if monitor.name in monitor_owner:
                raise FeedConfigError(
                    f"duplicate monitor name '{monitor.name}' in feeds "
                    f"'{monitor_owner[monitor.name]}' and '{feed.name}'")
            monitor_owner[monitor.name] = feed.name
        for layer in feed.typed_layers:
            if layer.table in typed_owner:
                raise FeedConfigError(
                    f"table '{layer.table}' is declared `typed: true` by both "
                    f"'{typed_owner[layer.table]}' and '{feed.name}'")
            typed_owner[layer.table] = feed.name
        for layer in feed.watched_layers:
            if layer.table in watch_owner:
                raise FeedConfigError(
                    f"table '{layer.table}' is watched by both "
                    f"'{watch_owner[layer.table]}' and '{feed.name}'")
            watch_owner[layer.table] = feed.name
    return feeds


def all_monitors(directory: Path | None = None) -> list[MonitorDef]:
    return [m for feed in load_feeds(directory) for m in feed.monitors]
