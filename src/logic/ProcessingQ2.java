package logic;

import entity.SommDonne;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import main.Main;

public class ProcessingQ2 {
	
public static Dataset<SommDonne> parseCsvSommDonne(SparkSession spark ){
		
		Dataset<Row> df = spark.read()
				.csv(Main.getFileSomministrazioneVacciniDonne());

		df = df.withColumnRenamed("_c2", "area");
		df = df.withColumnRenamed("_c0", "data");
				df =df.withColumn("mese_giorno", (df.col("data")).substr(6, 5));

		df = df.withColumnRenamed("_c3", "fascia");
		df = df.withColumnRenamed("_c5", "totale");

		df = df.select( "mese_giorno","area", "fascia", "totale" ).orderBy("area", "fascia", "mese_giorno").toDF();

		return df.as( Encoders.bean ( SommDonne.class ));
	}
}
