///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21
//DEPS org.apache.spark:spark-core_2.13:4.0.1
//DEPS org.apache.spark:spark-sql_2.13:4.0.1
//DEPS org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1

import static java.lang.System.*;

import java.io.Serializable;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class demo_020_streaming {

    public static void main(String... args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder().appName("010_dataset_demo").master("local[*]").getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9094")
                .option("subscribe", "commits")
                .option("startingOffsets", "earliest")
                .load();

        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream().format("console").start()
                .awaitTermination();
    }
}
