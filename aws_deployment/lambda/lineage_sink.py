"""Lambda: minimal OpenLineage receiver behind a Function URL.

Marquez is the real backend (lineage_deployment/), but it runs on a laptop
that Glue and EMR cannot reach. To verify that the AWS emitters actually
send well-formed events, this Lambda accepts ``POST /api/v1/lineage`` and
writes each RunEvent to CloudWatch Logs as one JSON line. ``deploy.sh``
prints the URL; point ``OPENLINEAGE_URL`` at it and the events show up in
the log group ``/aws/lambda/<project>-lineage-sink``. They can be replayed
into Marquez later (``lineage_deployment/replay_cloudwatch.py``).

It is a capture endpoint, not a lineage store: no auth (function URL
``AuthType: NONE``, gated by a template parameter, default off), no
persistence beyond the log retention.
"""

import base64
import json


def lambda_handler(event, context):
    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8", errors="replace")
    path = event.get("rawPath", "")
    try:
        run_event = json.loads(body) if body else {}
    except json.JSONDecodeError:
        print(json.dumps({"lineage_sink": "bad json", "path": path,
                          "body": body[:2000]}))
        return {"statusCode": 400, "body": "invalid JSON"}

    job = run_event.get("job", {})
    print(json.dumps({
        "lineage_sink": "event",
        "path": path,
        "eventType": run_event.get("eventType"),
        "job": f"{job.get('namespace')}/{job.get('name')}",
        "runId": run_event.get("run", {}).get("runId"),
        "inputs": [f"{d.get('namespace')}{d.get('name')}"
                   for d in run_event.get("inputs", [])],
        "outputs": [f"{d.get('namespace')}{d.get('name')}"
                    for d in run_event.get("outputs", [])],
        "producer": run_event.get("producer"),
        "event": run_event,
    }, default=str))
    # Marquez answers 201 Created; clients only check for 2xx.
    return {"statusCode": 201, "body": ""}
