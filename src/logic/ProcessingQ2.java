package logic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import main.Main;

public class ProcessingQ2 {
	
public static Dataset<Row> parseCsvSommDonne( SparkSession spark ){
		
		Dataset<Row> df = spark.read()
				.csv(Main.getFileSomministrazioneVacciniDonne());
		
		df = df.withColumnRenamed("_c2", "area");
		df = df.withColumnRenamed("_c0", "data");
				df =df.withColumn("mese", (df.col("data")).substr(6, 2));

		df = df.withColumnRenamed("_c3", "fascia");
		df = df.withColumnRenamed("_c5", "totale");

		df = df.select( "mese","area", "fascia", "totale" ).groupBy( "mese","area", "fascia" ).agg( df.col("mese"));
	    return df;
	}
}
