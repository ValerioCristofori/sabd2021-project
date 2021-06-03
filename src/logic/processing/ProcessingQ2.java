package logic.processing;

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
		Dataset<Row> df = Main.getHdfs().getDatasetInput("somministrazioni-vaccini-latest.csv");


		df = df.withColumnRenamed("_c0", "area");
		df = df.withColumnRenamed("_c1", "data");
		df = df.withColumnRenamed("_c2", "fascia");
		df = df.withColumnRenamed("_c3", "totale");

		df = df.select( "data","area", "fascia", "totale" ).orderBy("area", "fascia", "data").toDF();

		return df.as( Encoders.bean ( SommDonne.class ));
	}

public static Date getFilterDate( ) {
	// prendo in considerazione solo istanze di date a partire dal febb 2021
	Calendar febbraio2021 = Calendar.getInstance();
	febbraio2021.set(Calendar.YEAR, 2021);
	febbraio2021.set(Calendar.MONTH, Calendar.FEBRUARY);
	febbraio2021.set(Calendar.DAY_OF_MONTH, 1);
    return febbraio2021.getTime();
}
}
