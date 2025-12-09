///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21
//DEPS org.apache.spark:spark-core_2.13:4.0.1
//DEPS org.apache.spark:spark-sql_2.13:4.0.1
//DEPS org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1
//DEPS io.delta:delta-spark_2.13:4.0.0

import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class demo_030_streaming {

	public static void main(String... args) throws TimeoutException, StreamingQueryException {
		SparkSession spark = SparkSession.builder()
			.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
			.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
			.appName("030_streaming_demo")
			.master("local[*]")
			.getOrCreate();

		spark.sparkContext().setLogLevel("WARN");

		StructType schema = DataTypes.createStructType(List.of(
				DataTypes.createStructField("commitId", DataTypes.StringType, true),
				DataTypes.createStructField("shortMessage", DataTypes.StringType, true),
				DataTypes.createStructField("authorEmail", DataTypes.StringType, true),
				DataTypes.createStructField("committerEmail", DataTypes.StringType, true),
				DataTypes.createStructField("authoredAt", DataTypes.IntegerType, true),
				DataTypes.createStructField("committedAt", DataTypes.IntegerType, true),
				DataTypes.createStructField("oldPath", DataTypes.StringType, true),
				DataTypes.createStructField("newPath", DataTypes.StringType, true),
				DataTypes.createStructField("linesInserted", DataTypes.IntegerType, true),
				DataTypes.createStructField("linesDeleted", DataTypes.IntegerType, true),
				DataTypes.createStructField("changeType", DataTypes.StringType, true)));

		Dataset<Row> df = spark.readStream()
			.format("kafka")
			.option("kafka.bootstrap.servers", "localhost:9094")
			.option("subscribe", "my-git-source-topic")
			.option("startingOffsets", "earliest")
			.load();

		Dataset<GitSourceCommitChange> ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
			.select(functions.from_json(new Column("value"), schema).alias("data"))
			.select("data.*")
			.as(ExpressionEncoder.javaBean(GitSourceCommitChange.class));

		// ds.writeStream()
		// 	.format("delta")
		// 	.option("overwriteSchema", "true")
		// 	.option("delta.enableChangeDataFeed", true)
		// 	.option("checkpointLocation", "/tmp/checkpoints/commits")
		// 	.start("/tmp/delta/commits")
		// 	.awaitTermination();

		ds.writeStream()
			.foreachBatch((batchDs, batchId) -> {
				batchDs.write()
					.format("delta")
					.mode(SaveMode.Overwrite)
					.option("overwriteSchema", "true")
					.option("delta.enableChangeDataFeed", true)
					.option("checkpointLocation", "/tmp/checkpoints/commits")
					.save("/tmp/delta/commits");
				batchDs.write().format("console");
				batchDs.show();
			})
			.start()
			.awaitTermination();
	}

	public static class GitSourceCommitChange {
		private String commitId;
		private String shortMessage;
		private String authorEmail;
		private String committerEmail;
		private Integer authoredAt; // actually an Instant
		private Integer committedAt; // actually an Instant
		private String oldPath;
		private String newPath;
		private Integer linesInserted;
		private Integer linesDeleted;
		private String changeType;

		public GitSourceCommitChange() {
		}

		public GitSourceCommitChange(String commitId, String shortMessage, String authorEmail, String committerEmail,
				Integer authoredAt, Integer committedAt, String oldPath, String newPath, Integer linesInserted,
				Integer linesDeleted, String changeType) {
			this.commitId = commitId;
			this.shortMessage = shortMessage;
			this.authorEmail = authorEmail;
			this.committerEmail = committerEmail;
			this.authoredAt = authoredAt;
			this.committedAt = committedAt;
			this.oldPath = oldPath;
			this.newPath = newPath;
			this.linesInserted = linesInserted;
			this.linesDeleted = linesDeleted;
			this.changeType = changeType;
		}

		public Integer getLinesDeleted() {
			return linesDeleted;
		}

		public void setLinesDeleted(Integer linesDeleted) {
			this.linesDeleted = linesDeleted;
		}

		public String getCommitId() {
			return commitId;
		}

		public void setCommitId(String commitId) {
			this.commitId = commitId;
		}

		public String getShortMessage() {
			return shortMessage;
		}

		public void setShortMessage(String shortMessage) {
			this.shortMessage = shortMessage;
		}

		public String getAuthorEmail() {
			return authorEmail;
		}

		public void setAuthorEmail(String authorEmail) {
			this.authorEmail = authorEmail;
		}

		public String getCommitterEmail() {
			return committerEmail;
		}

		public void setCommitterEmail(String committerEmail) {
			this.committerEmail = committerEmail;
		}

		public Integer getAuthoredAt() {
			return authoredAt;
		}

		public void setAuthoredAt(Integer authoredAt) {
			this.authoredAt = authoredAt;
		}

		public Integer getCommittedAt() {
			return committedAt;
		}

		public void setCommittedAt(Integer committedAt) {
			this.committedAt = committedAt;
		}

		public String getOldPath() {
			return oldPath;
		}

		public void setOldPath(String oldPath) {
			this.oldPath = oldPath;
		}

		public String getNewPath() {
			return newPath;
		}

		public void setNewPath(String newPath) {
			this.newPath = newPath;
		}

		public Integer getLinesInserted() {
			return linesInserted;
		}

		public void setLinesInserted(Integer linesInserted) {
			this.linesInserted = linesInserted;
		}

		public String getChangeType() {
			return changeType;
		}

		public void setChangeType(String changeType) {
			this.changeType = changeType;
		}
	}
}
