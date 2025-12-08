///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21
//DEPS org.apache.spark:spark-core_2.13:4.0.1
//DEPS org.apache.spark:spark-sql_2.13:4.0.1
//DEPS org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1
//DEPS io.delta:delta-spark_2.13:4.0.0

import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import io.delta.tables.DeltaTable;

public class demo_020_delta {

	public static void main(String... args) throws TimeoutException, StreamingQueryException {
		SparkSession spark = SparkSession.builder()
			.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
			.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
			.appName("020_delta_demo")
			.master("local[*]")
			.getOrCreate();

		spark.sparkContext().setLogLevel("WARN");

		spark.createDataset(List.of("hello", "world"), Encoders.STRING())
			.write()
			.format("delta")
			.mode("overwrite")
			.option("overwriteSchema", "true")
			.option("delta.enableChangeDataFeed", true)
			.save("/tmp/delta/texts");

		spark.createDataset(List.of("and", "openvalue", "!"), Encoders.STRING())
			.write()
			.format("delta")
			.mode("overwrite")
			.option("overwriteSchema", "true")
			.option("delta.enableChangeDataFeed", true)
			.save("/tmp/delta/texts");

		DeltaTable.forPath(spark, "/tmp/delta/texts").history().show();

		spark.read().format("delta").option("readChangeFeed", "true").option("startingVersion", 0).load("/tmp/delta/texts").show();

		spark.read().format("delta").option("versionAsOf", 0).load("/tmp/delta/texts").show();

		spark.read().format("delta").option("versionAsOf", 1).load("/tmp/delta/texts").show();
	}
}
