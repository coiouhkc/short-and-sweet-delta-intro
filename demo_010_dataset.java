///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21
//DEPS org.apache.spark:spark-core_2.13:4.0.1
//DEPS org.apache.spark:spark-sql_2.13:4.0.1
//DEPS org.apache.spark:spark-streaming-kafka-0-10-assembly_2.13:4.0.1

package me.abratuhin.demo.spark;

import static java.lang.System.*;

import java.io.Serializable;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class demo_010_dataset {

    public static void main(String... args) {
        SparkSession spark = SparkSession.builder().appName("010_dataset_demo").master("local[*]").getOrCreate();

        spark.createDataset(List.of("Hello", "world"), Encoders.STRING()).show();

        // Create a DataFrame
        Dataset<MyGitCommit> df = spark.createDataset(
                List.of(
                        new MyGitCommit("123456", "emil.tischbein", 10, 0),
                        new MyGitCommit("67890a", "pony.huetchen", 9, 123)),
                Encoders.bean(MyGitCommit.class));
        df.show();

        // Stop the Spark session
        spark.stop();
    }

    // public static record MyGitCommit(
    // String hash, String committer, Integer modified, Integer deleted) {
    // }

    public static class MyGitCommit implements Serializable {
        String hash;
        String committer;
        Integer modified;
        Integer deleted;

        public MyGitCommit() {
        }

        public MyGitCommit(String hash, String committer, Integer modified, Integer deleted) {
            this.hash = hash;
            this.committer = committer;
            this.modified = modified;
            this.deleted = deleted;
        }

        public String getHash() {
            return hash;
        }

        public String getCommitter() {
            return committer;
        }

        public Integer getModified() {
            return modified;
        }

        public Integer getDeleted() {
            return deleted;
        }

        public void setHash(String hash) {
            this.hash = hash;
        }

        public void setCommitter(String committer) {
            this.committer = committer;
        }

        public void setModified(Integer modified) {
            this.modified = modified;
        }

        public void setDeleted(Integer deleted) {
            this.deleted = deleted;
        }
    }
}
