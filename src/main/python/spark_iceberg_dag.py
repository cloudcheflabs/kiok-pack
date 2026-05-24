"""Kiok DAG (Python SDK) — same workload as the YAML twin.
One ssh-then-spark-submit per task; pyspark script + java uberjar both live
in S3. Credentials only via conn(...).
Prereq:
  * passwordless SSH kiok@worker -> spark@spark-master
  * s3://pyspark-scripts/py_pyspark.py uploaded once
"""
from kiok import Dag, conn

SSH = ('ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null '
       '-o LogLevel=ERROR spark@172.31.12.163')

# Pre-built credential refs so the f-string inside the script body stays clean
# and regex-friendly (the inner conn() call has its own single quotes that
# break naive ' '-delimited parsing).
POLARIS_CRED = conn('polaris-rest', 'oauthCredential')
S3_ENDPOINT  = conn('s3-shannon', 'endpoint')
S3_AK        = conn('s3-shannon', 'accessKey')
S3_SK        = conn('s3-shannon', 'secretKey')

CLIENT_SCRIPT = f"""#!/bin/bash
set -euo pipefail
SSH="{SSH}"
$SSH "
  export JAVA_HOME=/opt/openlogic-openjdk-17.0.7+7-linux-x64
  export POLARIS_URI='http://172.31.12.163:30000/api/catalog'
  export POLARIS_CRED='{POLARIS_CRED}'
  export S3_ENDPOINT='{S3_ENDPOINT}'
  export S3_AK='{S3_AK}'
  export S3_SK='{S3_SK}'
  /opt/components/spark-main-master-1/bin/spark-submit \\
    --master spark://172.31.12.163:8780 \\
    --deploy-mode client --conf spark.eventLog.enabled=false \\
    --driver-java-options '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED' \\
    --conf spark.executor.extraJavaOptions='--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED' \\
    --conf spark.hadoop.fs.s3a.endpoint='{S3_ENDPOINT}' \\
    --conf spark.hadoop.fs.s3a.access.key='{S3_AK}' \\
    --conf spark.hadoop.fs.s3a.secret.key='{S3_SK}' \\
    --conf spark.hadoop.fs.s3a.path.style.access=true \\
    s3a://pyspark-scripts/py_pyspark.py
"
"""

CLUSTER_SCRIPT = f"""#!/bin/bash
set -euo pipefail
SSH="{SSH}"
$SSH "
  export JAVA_HOME=/opt/openlogic-openjdk-17.0.7+7-linux-x64
  /opt/components/spark-main-master-1/bin/spark-submit \\
    --master spark://172.31.12.163:8780 \\
    --deploy-mode cluster --conf spark.eventLog.enabled=false \\
    --class com.cloudcheflabs.spark.IcebergSmoke \\
    --conf spark.driver.extraJavaOptions='--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED' \\
    --conf spark.executor.extraJavaOptions='--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED' \\
    --conf spark.hadoop.fs.s3a.endpoint='{S3_ENDPOINT}' \\
    --conf spark.hadoop.fs.s3a.access.key='{S3_AK}' \\
    --conf spark.hadoop.fs.s3a.secret.key='{S3_SK}' \\
    --conf spark.hadoop.fs.s3a.path.style.access=true \\
    s3a://uberjar-upload/iceberg-smoke-java.jar
"
"""

dag = Dag("spark-iceberg-python", catchup=False, default_timeout="10m")
dag.task("pyspark_client_mode_write", script=CLIENT_SCRIPT)
dag.task("java_spark_cluster_mode_write", script=CLUSTER_SCRIPT,
         requires=["pyspark_client_mode_write"])
