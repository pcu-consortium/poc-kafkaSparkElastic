package kafkaReader;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {

		// Init des éléments de base
		SparkConf sparkConf = new SparkConf().setAppName("kafkaSparkElastic").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		SparkSession ss = SparkSession.builder().getOrCreate();

		// on lit le flux du kafka *EN BATCH*
		Dataset<Row> ds1 = ss.read().format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", "topic1").load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)",
						"topic", "partition", "offset", "timestamp", "timestampType");

		ds1.show(50, false);
		// ds2.show(50, false);
	}
}
