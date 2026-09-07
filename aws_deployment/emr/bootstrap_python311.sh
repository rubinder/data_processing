#!/usr/bin/env bash
# EMR bootstrap action: install Python 3.11 for PySpark.
#
# EMR 7.x (Amazon Linux 2023) ships Python 3.9 as the default interpreter.
# spark_applications is written for the project's Python 3.10 baseline
# (PEP 604 `X | None` annotations evaluated at import time, dataclass field
# types, ...), so the jobs need a newer interpreter on every node. AL2023
# packages python3.11; pandas + pyarrow are needed by the pandas-UDF path
# (debugging case 07) and by Arrow-based collect.
#
# Referenced from cloudformation/main.yaml when EmrBootstrapScriptKey is set;
# scripts/deploy.sh uploads this file to the deployment bucket.
set -euo pipefail

sudo dnf install -y python3.11 python3.11-pip
sudo python3.11 -m pip install --quiet --no-cache-dir \
    "pandas>=1.5,<2.3" "pyarrow>=4,<17" "setuptools<80"
python3.11 --version
