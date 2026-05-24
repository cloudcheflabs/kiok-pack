package com.cloudcheflabs.dags;

import com.cloudcheflabs.kiok.sdk.Conn;
import com.cloudcheflabs.kiok.sdk.Dag;
import com.cloudcheflabs.kiok.sdk.KiokDag;

/**
 * Kiok DAG (Java SDK) — same workload as the YAML / Python twins. One ssh
 * then one spark-submit per task; pyspark script (java_pyspark.py) + java
 * uberjar both live in S3. Credentials only via {@link Conn#ref} references.
 * Prereq:
 *   * passwordless SSH kiok@worker -> spark@spark-master
 *   * s3://pyspark-scripts/java_pyspark.py uploaded once
 */
public class SparkIcebergJavaDag implements KiokDag {

    private static final String SSH =
            "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
          + "-o LogLevel=ERROR spark@172.31.12.163";

    private static final String POLARIS_CRED = Conn.ref("polaris-rest", "oauthCredential");
    private static final String S3_ENDPOINT  = Conn.ref("s3-shannon",   "endpoint");
    private static final String S3_AK        = Conn.ref("s3-shannon",   "accessKey");
    private static final String S3_SK        = Conn.ref("s3-shannon",   "secretKey");

    private static final String CLIENT_SCRIPT = """
            #!/bin/bash
            set -euo pipefail
            SSH="%s"
            $SSH "
              export JAVA_HOME=/opt/openlogic-openjdk-17.0.7+7-linux-x64
              export POLARIS_URI='http://172.31.12.163:30000/api/catalog'
              export POLARIS_CRED='%s'
              export S3_ENDPOINT='%s'
              export S3_AK='%s'
              export S3_SK='%s'
              /opt/components/spark-main-master-1/bin/spark-submit \\
                --master spark://172.31.12.163:8780 \\
                --deploy-mode client --conf spark.eventLog.enabled=false \\
                --driver-java-options '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED' \\
                --conf spark.executor.extraJavaOptions='--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED' \\
                --conf spark.hadoop.fs.s3a.endpoint='%s' \\
                --conf spark.hadoop.fs.s3a.access.key='%s' \\
                --conf spark.hadoop.fs.s3a.secret.key='%s' \\
                --conf spark.hadoop.fs.s3a.path.style.access=true \\
                s3a://pyspark-scripts/java_pyspark.py
            "
            """.formatted(SSH,
                    POLARIS_CRED, S3_ENDPOINT, S3_AK, S3_SK,
                    S3_ENDPOINT, S3_AK, S3_SK);

    private static final String CLUSTER_SCRIPT = """
            #!/bin/bash
            set -euo pipefail
            SSH="%s"
            $SSH "
              export JAVA_HOME=/opt/openlogic-openjdk-17.0.7+7-linux-x64
              /opt/components/spark-main-master-1/bin/spark-submit \\
                --master spark://172.31.12.163:8780 \\
                --deploy-mode cluster --conf spark.eventLog.enabled=false \\
                --class com.cloudcheflabs.spark.IcebergSmoke \\
                --conf spark.driver.extraJavaOptions='--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED' \\
                --conf spark.executor.extraJavaOptions='--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED' \\
                --conf spark.hadoop.fs.s3a.endpoint='%s' \\
                --conf spark.hadoop.fs.s3a.access.key='%s' \\
                --conf spark.hadoop.fs.s3a.secret.key='%s' \\
                --conf spark.hadoop.fs.s3a.path.style.access=true \\
                s3a://uberjar-upload/iceberg-smoke-java.jar
            "
            """.formatted(SSH, S3_ENDPOINT, S3_AK, S3_SK);

    @Override
    public Dag define() {
        Dag dag = new Dag("spark-iceberg-java")
                .catchup(false)
                .defaultTimeoutMs(600_000);
        dag.task("pyspark_client_mode_write").shell(CLIENT_SCRIPT);
        dag.task("java_spark_cluster_mode_write")
           .requires("pyspark_client_mode_write")
           .shell(CLUSTER_SCRIPT);
        return dag;
    }
}
