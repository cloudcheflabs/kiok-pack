package com.cloudcheflabs.kiokpack;

/**
 * Example kiok {@code type: java} task.
 *
 * <p>A DAG bundle is a pre-built jar committed at {@code build/dag.jar}. kiok's
 * git-sync distributes it to every worker; a {@code type: java} task names the
 * main class in {@code config.class} and the worker runs it as a subprocess.
 */
public class HelloJavaTask {
    public static void main(String[] args) {
        System.out.println("hello from a kiok type:java task");
        System.out.println("running on java " + System.getProperty("java.version"));
        int sum = 0;
        for (int i = 1; i <= 10; i++) {
            sum += i;
        }
        System.out.println("sum(1..10) = " + sum);
        System.exit(0);
    }
}
