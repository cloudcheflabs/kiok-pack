package com.cloudcheflabs.kiokpack;

import com.cloudcheflabs.kiok.sdk.Dag;
import com.cloudcheflabs.kiok.sdk.KiokDag;

/**
 * A kiok DAG authored in Java code — the Java counterpart of a YAML or Python
 * DAG. kiok's git-sync / bundle ingestion compiles every {@link KiokDag} class
 * in {@code build/dag.jar} into a DAG definition.
 */
public class JavaPipelineDag implements KiokDag {

    @Override
    public Dag define() {
        Dag dag = new Dag("java_pipeline").schedule("0 5 * * *");
        dag.task("extract")
                .shell("#!/bin/bash\necho 'java DAG: extract step'");
        dag.task("transform").requires("extract")
                .python("print('java DAG: transform step', sum(range(1, 6)))");
        dag.task("load").requires("transform")
                .shell("#!/bin/bash\necho 'java DAG: load step'");
        return dag;
    }
}
