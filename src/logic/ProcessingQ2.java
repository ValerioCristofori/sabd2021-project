package logic;

import entity.SommDonne;

import java.util.Calendar;
import java.util.Date;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import main.Main;

public class ProcessingQ2 {
	
public static Dataset<SommDonne> parseCsvSommDonne(SparkSession spark ){
		// creo dataset con le colonne (area, data, fascia, totale) dal file csv
	    // e lo ordino per (area, fascia, data)
		Dataset<Row> df = spark.read()
				.csv(Main.getFileSomministrazioneVacciniDonne());

		df = df.withColumnRenamed("_c2", "area");
		df = df.withColumnRenamed("_c0", "data");
		//df =df.withColumn("mese_giorno", (df.col("data")).substr(6, 5));

		df = df.withColumnRenamed("_c3", "fascia");
		df = df.withColumnRenamed("_c5", "totale");

		df = df.select( "data","area", "fascia", "totale" ).orderBy("area", "fascia", "data").toDF();

		return df.as( Encoders.bean ( SommDonne.class ));
	}

public static Date getFilterDate( ) {
	// prendo in considerazione solo istanze di date del 2021
	Calendar gennaio2021 = Calendar.getInstance();
	gennaio2021.set(Calendar.YEAR, 2021);
    gennaio2021.set(Calendar.MONTH, Calendar.JANUARY);
    gennaio2021.set(Calendar.DAY_OF_MONTH, 1);
    return gennaio2021.getTime(); 
}
}
