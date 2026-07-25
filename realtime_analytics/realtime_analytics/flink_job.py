"""PyFlink streaming job: Kafka conversation events -> ClickHouse.

Two outputs from one source:

1. raw events, appended to the ClickHouse events table;
2. a 1-minute event-time tumbling rollup for the live operational view.

Everything tunable is collected in :class:`FlinkTuning` with an environment
override, and the *reasoning* for each default is in the docstrings below and
in the "Flink tuning" section of README.md. The tuning object and the config
map it produces are plain Python, so the test suite asserts on them without
starting a cluster.

Submit with::

    flink run -py realtime_analytics/flink_job.py
    # or, against the cluster in this module:
    ./deploy.sh submit
"""
import os
from dataclasses import dataclass, field

from realtime_analytics import flink_sql


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _env_bool(name: str, default: bool) -> bool:
    return os.environ.get(name, str(default)).lower() in {"1", "true", "yes"}


@dataclass
class FlinkTuning:
    """Every knob that was actually tuned, with the reasoning for its value."""

    # --- Parallelism -------------------------------------------------------
    #: Default parallelism. The useful ceiling for a Kafka-sourced job is the
    #: partition count: subtasks beyond that sit idle holding slots, and -- if
    #: any operator is event-time windowed -- an idle subtask emits no
    #: watermark, so the whole job's watermark stalls and windows never fire.
    #: That failure looks like "the pipeline is up but the dashboard stopped
    #: updating", which is why source idleness is configured below.
    #: Scale up when a subtask is CPU-bound (busy ratio near 1.0), not when
    #: lag is high -- lag caused by a slow sink gets worse with more
    #: parallelism, because more subtasks contend for the same sink.
    parallelism: int = field(
        default_factory=lambda: _env_int("FLINK_PARALLELISM", 4)
    )
    #: Sink parallelism is pinned lower on purpose. ClickHouse wants a few
    #: large inserts, not many small ones; N sink subtasks means N times the
    #: parts per interval. This is the knob that decides part count.
    sink_parallelism: int = field(
        default_factory=lambda: _env_int("FLINK_SINK_PARALLELISM", 2)
    )

    # --- Checkpointing -----------------------------------------------------
    #: Checkpoint interval. Shorter means less replay after a failure but more
    #: steady-state overhead; 30s keeps recovery under a minute for this job.
    checkpoint_interval_ms: int = field(
        default_factory=lambda: _env_int("FLINK_CHECKPOINT_INTERVAL_MS", 30_000)
    )
    #: Minimum quiet time between checkpoints. Without it, a job that takes
    #: 25s to checkpoint on a 30s interval spends nearly all its time
    #: checkpointing and never drains its backlog. This is the single setting
    #: that most often turns a struggling job into a stuck one.
    checkpoint_min_pause_ms: int = field(
        default_factory=lambda: _env_int("FLINK_CHECKPOINT_MIN_PAUSE_MS", 10_000)
    )
    checkpoint_timeout_ms: int = field(
        default_factory=lambda: _env_int("FLINK_CHECKPOINT_TIMEOUT_MS", 120_000)
    )
    #: Tolerate transient checkpoint failures (a brief S3/HDFS blip) instead
    #: of failing the job and replaying from the last successful checkpoint.
    tolerable_checkpoint_failures: int = field(
        default_factory=lambda: _env_int("FLINK_TOLERABLE_CP_FAILURES", 3)
    )
    #: Unaligned checkpoints: the direct remedy for backpressure stalling
    #: checkpoints. Under backpressure, barriers queue behind buffered data
    #: and aligned checkpoints time out; unaligned checkpoints let barriers
    #: overtake in-flight data at the cost of storing those buffers in the
    #: checkpoint. Enable when checkpoint duration tracks backpressure;
    #: leave off for a job that is never backpressured, since it inflates
    #: checkpoint size.
    unaligned_checkpoints: bool = field(
        default_factory=lambda: _env_bool("FLINK_UNALIGNED_CHECKPOINTS", True)
    )

    # --- State backend -----------------------------------------------------
    #: ``rocksdb`` or ``hashmap``.
    #:
    #: hashmap  -- state lives on the JVM heap. Fastest possible access, no
    #:             serialization on the hot path. Correct choice when state
    #:             fits comfortably in memory: this job's 1-minute windows
    #:             over a few thousand keys are a few hundred MB at most.
    #:             The failure mode is a GC death spiral, then OOM.
    #: rocksdb  -- state spills to local disk, so it scales past memory, and
    #:             it is the only backend supporting *incremental*
    #:             checkpoints, which matters enormously once state reaches
    #:             tens of GB. Costs serialization on every access; expect
    #:             single-digit-x lower throughput on state-heavy operators.
    #:
    #: Default is rocksdb because the production version of this job keeps
    #: per-conversation session state with a multi-hour timeout, where state
    #: size is unbounded by anything the job controls. For the windowed
    #: rollup alone, hashmap is measurably faster -- see README.
    state_backend: str = field(
        default_factory=lambda: os.environ.get("FLINK_STATE_BACKEND", "rocksdb")
    )
    incremental_checkpoints: bool = field(
        default_factory=lambda: _env_bool("FLINK_INCREMENTAL_CHECKPOINTS", True)
    )
    checkpoint_dir: str = field(
        default_factory=lambda: os.environ.get(
            "FLINK_CHECKPOINT_DIR", "file:///tmp/flink-checkpoints"
        )
    )

    # --- Watermarks / windows ---------------------------------------------
    max_out_of_orderness_s: int = field(
        default_factory=lambda: _env_int(
            "FLINK_MAX_OUT_OF_ORDERNESS_S",
            flink_sql.DEFAULT_MAX_OUT_OF_ORDERNESS_S,
        )
    )
    #: A partition with no traffic never advances its watermark, which holds
    #: back the global watermark (it is the minimum across subtasks) and
    #: freezes every downstream window. Marking a quiet source idle after this
    #: long excludes it from the watermark calculation.
    source_idle_timeout_ms: int = field(
        default_factory=lambda: _env_int("FLINK_SOURCE_IDLE_TIMEOUT_MS", 60_000)
    )
    window_minutes: int = field(
        default_factory=lambda: _env_int("FLINK_WINDOW_MINUTES", 1)
    )

    # --- Throughput --------------------------------------------------------
    #: Mini-batch aggregation buffers records and does one state access per
    #: key per batch instead of per record. On a keyed aggregation this is
    #: typically the largest single throughput win available, and it costs
    #: exactly ``mini_batch_latency_ms`` of extra latency.
    mini_batch_enabled: bool = field(
        default_factory=lambda: _env_bool("FLINK_MINI_BATCH", True)
    )
    mini_batch_latency_ms: int = field(
        default_factory=lambda: _env_int("FLINK_MINI_BATCH_LATENCY_MS", 1_000)
    )
    mini_batch_size: int = field(
        default_factory=lambda: _env_int("FLINK_MINI_BATCH_SIZE", 20_000)
    )
    #: Two-phase (local-global) aggregation pre-aggregates before the shuffle.
    #: With one tenant holding a large share of traffic, the single-phase plan
    #: sends every record for that tenant to one subtask; two-phase collapses
    #: them locally first and is the standard answer to a hot key.
    two_phase_aggregation: bool = field(
        default_factory=lambda: _env_bool("FLINK_TWO_PHASE_AGG", True)
    )

    # --- Restart strategy --------------------------------------------------
    restart_attempts: int = field(
        default_factory=lambda: _env_int("FLINK_RESTART_ATTEMPTS", 10)
    )
    restart_delay_ms: int = field(
        default_factory=lambda: _env_int("FLINK_RESTART_DELAY_MS", 10_000)
    )


def table_config_options(tuning: FlinkTuning) -> dict:
    """Flink table-planner options derived from the tuning object.

    Returned as a plain dict so it can be asserted on in tests and logged at
    startup -- a job whose effective configuration is invisible is a job
    nobody can tune twice.
    """
    options = {
        "table.exec.source.idle-timeout": f"{tuning.source_idle_timeout_ms}ms",
        "table.exec.mini-batch.enabled": str(tuning.mini_batch_enabled).lower(),
        "table.optimizer.agg-phase-strategy": (
            "TWO_PHASE" if tuning.two_phase_aggregation else "AUTO"
        ),
        "table.exec.resource.default-parallelism": str(tuning.parallelism),
    }
    if tuning.mini_batch_enabled:
        options["table.exec.mini-batch.allow-latency"] = (
            f"{tuning.mini_batch_latency_ms}ms"
        )
        options["table.exec.mini-batch.size"] = str(tuning.mini_batch_size)
    return options


def configure_environment(tuning: FlinkTuning):
    """Build and configure the StreamExecutionEnvironment."""
    from pyflink.common import RestartStrategies
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.checkpointing_mode import CheckpointingMode
    from pyflink.datastream.state_backend import (
        EmbeddedRocksDBStateBackend,
        HashMapStateBackend,
    )

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(tuning.parallelism)

    env.enable_checkpointing(
        tuning.checkpoint_interval_ms, CheckpointingMode.EXACTLY_ONCE
    )
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_min_pause_between_checkpoints(
        tuning.checkpoint_min_pause_ms
    )
    checkpoint_config.set_checkpoint_timeout(tuning.checkpoint_timeout_ms)
    # One checkpoint at a time: concurrent checkpoints hide the fact that
    # checkpointing is not keeping up.
    checkpoint_config.set_max_concurrent_checkpoints(1)
    checkpoint_config.set_tolerable_checkpoint_failure_number(
        tuning.tolerable_checkpoint_failures
    )
    checkpoint_config.enable_unaligned_checkpoints(tuning.unaligned_checkpoints)
    # Keep checkpoints after cancellation so a job can be resumed deliberately
    # rather than restarting from the topic's retention horizon.
    try:
        from pyflink.datastream.checkpoint_config import ExternalizedCheckpointCleanup

        checkpoint_config.set_externalized_checkpoint_cleanup(
            ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        )
    except ImportError:  # pragma: no cover - name moved across Flink versions
        pass
    checkpoint_config.set_checkpoint_storage_dir(tuning.checkpoint_dir)

    if tuning.state_backend == "rocksdb":
        env.set_state_backend(
            EmbeddedRocksDBStateBackend(tuning.incremental_checkpoints)
        )
    else:
        env.set_state_backend(HashMapStateBackend())

    env.set_restart_strategy(
        RestartStrategies.fixed_delay_restart(
            tuning.restart_attempts, tuning.restart_delay_ms
        )
    )
    return env


def build_job(tuning: FlinkTuning | None = None):
    """Wire the Kafka source and the rollup sink into a StatementSet.

    Flink is responsible for exactly one thing here: the event-time windowed
    rollup, which is the part ClickHouse cannot do (it has no watermarks and
    no notion of a late event). Raw events are *not* routed through Flink --
    ClickHouse ingests them straight off the same topic with its Kafka table
    engine, or the plain Python consumer does. Passing raw rows through Flink
    would add a hop, a failure mode, and a duplicate-delivery problem while
    buying nothing.
    """
    from pyflink.table import StreamTableEnvironment

    tuning = tuning or FlinkTuning()
    env = configure_environment(tuning)
    t_env = StreamTableEnvironment.create(env)

    config = t_env.get_config().get_configuration()
    for key, value in table_config_options(tuning).items():
        config.set_string(key, value)

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    topic = os.getenv("EVENTS_TOPIC", "conversation.events")
    group = os.getenv("FLINK_CONSUMER_GROUP", "flink-conversation-events")
    agg_topic = os.getenv("MINUTE_AGG_TOPIC", "conversation.minute_agg")

    t_env.execute_sql(
        flink_sql.kafka_source_ddl(
            topic, bootstrap, group, tuning.max_out_of_orderness_s
        )
    )
    t_env.execute_sql(flink_sql.minute_agg_sink_ddl(agg_topic, bootstrap))

    statements = t_env.create_statement_set()
    statements.add_insert_sql(flink_sql.minute_agg_sql(tuning.window_minutes))
    return t_env, statements, tuning


def run() -> None:
    t_env, statements, tuning = build_job()
    print("Flink tuning in effect:", flush=True)
    for key, value in sorted(vars(tuning).items()):
        print(f"  {key} = {value}", flush=True)
    # Detached: submit and return, rather than blocking the client forever on
    # an unbounded streaming job.
    statements.execute()
    print("Job submitted.", flush=True)


if __name__ == "__main__":
    run()
