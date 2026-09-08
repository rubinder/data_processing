"""Replay OpenLineage events captured by the AWS lineage sink into Marquez.

The sink Lambda (aws_deployment/lambda/lineage_sink.py) logs every RunEvent
it receives as one JSON line in CloudWatch. Glue and EMR cannot reach a
laptop's Marquez, so this pulls those lines and POSTs the embedded events to
a Marquez you can reach, preserving order and run ids. The result is the real
AWS lineage graph rendered locally.

    uv run --with boto3 python replay_cloudwatch.py \
        --log-group /aws/lambda/data-processing-lineage-sink \
        --since-hours 6 \
        --marquez http://localhost:5005

Reads with the default AWS CLI credential chain (needs botocore[crt] for an
`aws login` profile: uv run --with 'boto3' --with 'botocore[crt]' ...).
"""

import argparse
import json
import time
import urllib.request

import boto3


def captured_events(log_group: str, since_hours: float) -> list[dict]:
    logs = boto3.client("logs")
    start = int((time.time() - since_hours * 3600) * 1000)
    events = []
    paginator = logs.get_paginator("filter_log_events")
    for page in paginator.paginate(
        logGroupName=log_group, startTime=start,
        filterPattern='{ $.lineage_sink = "event" }',
    ):
        for record in page.get("events", []):
            try:
                events.append(json.loads(record["message"])["event"])
            except (KeyError, json.JSONDecodeError):
                continue
    events.sort(key=lambda e: e.get("eventTime", ""))
    return events


def post(marquez: str, event: dict) -> int:
    request = urllib.request.Request(
        f"{marquez.rstrip('/')}/api/v1/lineage",
        data=json.dumps(event).encode(),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return response.status


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--log-group", required=True)
    parser.add_argument("--since-hours", type=float, default=6.0)
    parser.add_argument("--marquez", default="http://localhost:5005")
    parser.add_argument("--dry-run", action="store_true",
                        help="list the events without posting")
    args = parser.parse_args(argv)

    events = captured_events(args.log_group, args.since_hours)
    print(f"{len(events)} events captured")
    for event in events:
        job = event.get("job", {})
        line = (f"{event.get('eventTime')} {event.get('eventType'):9} "
                f"{job.get('namespace')}/{job.get('name')}")
        if args.dry_run:
            print("  ", line)
            continue
        status = post(args.marquez, event)
        print(f"  {status} {line}")


if __name__ == "__main__":
    main()
