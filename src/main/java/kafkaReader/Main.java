package kafkaReader;

import java.util.regex.Pattern;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
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

		Logger log = LogManager.getRootLogger();

		// on lit le flux du kafka *EN BATCH*
		Dataset<Row> ds1 = ss.read().format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", "topic1").load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)",
						"topic", "partition", "offset", "timestamp", "timestampType");

		JavaRDD<String> jrdd2 = ds1.select(ds1.col("value")).toJavaRDD().map(v1 -> v1.mkString());

		Dataset<Row> ds2 = ss.read().json(jrdd2);

		ds1.show(50, false);
		ds2.show(50, false);

		// *****************************************************************
		// ~~~~~~~Pour sauvegarder un message JSON qui vient de kafka~~~~~~~
		// *****************************************************************

		// C'est utilisable à partir du moment où on a tout l'objet JSON dans
		// UNE COLONNE d'un dataframe

		// On transforme le dataframe en RDD<Row> qu'on transforme en RDD de
		// string
		// JavaRDD<String> sortieElasticJSON =
		// ds1.select(ds1.col("value")).toJavaRDD().map(v1 -> v1.mkString());
		// JavaEsSpark.saveJsonToEs(sortieElasticJSON, "sparkDepuisKafka/test");

		// *****************************************************************
		// ~~~~~~~~~~~~~~~~~~Pour sauvegarder un dataframe~~~~~~~~~~~~~~~~~~
		// *****************************************************************

		// On sauvegarde dans elastic
		// EsSparkSQL.saveToEs(ds1.select("value"),
		// "sparkDepuisDataframe/test");
		// JavaEsSpark.saveToEs(sortieElasticJSON, arg1);
	}

}
