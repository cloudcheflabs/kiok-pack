"""A kiok DAG authored in Python code — the Python counterpart of a YAML DAG
(the way an Airflow DAG is a Python file).

kiok's git-sync runs ``python3 -m kiok.compile`` on this file; every module-level
``Dag`` instance becomes a registered DAG.
"""
from kiok import Dag

dag = Dag("python_pipeline", schedule="0 6 * * *")
dag.task("extract", script="#!/bin/bash\necho 'python DAG: extract step'")
dag.task("compute", script="print('python DAG: compute step', sum(range(1, 11)))",
         task_type="python", requires=["extract"])
dag.task("load", script="#!/bin/bash\necho 'python DAG: load step'", requires=["compute"])
