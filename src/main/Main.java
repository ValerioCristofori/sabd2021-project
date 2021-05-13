package main;

import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import entity.CentriSomministrazione;
import logic.processing.Query1Processing;
import parser.CentriSomministrazioneParser;
import scala.Tuple2;
import scala.Tuple3;

public class Main {

	private static String filePuntiTipologia = "data/punti-somministrazione-tipologia.csv";
	private static String fileSomministrazioneVaccini = "data/somministrazioni-vaccini-summary-latest.csv";
			
	public static void main(String[] args) {
		
		query1();
	}

	private static void query1() {
		
		
		SparkConf conf = new SparkConf().setAppName("Query1");
		JavaSparkContext sc= new JavaSparkContext(conf);
		
		SparkSession spark = SparkSession
			    .builder()
			    .appName("Java Spark SQL Query1")
			    .getOrCreate();
		
		/*
		 *  prendere coppie -> Area, Numero di centri
		 */
		Dataset<Row> dfCentri = Query1Processing.getTotalCenters(spark);
		dfCentri = Query1Processing.getTotalCenters(spark, dfCentri);
		/*
		 *  prendere triple -> Data, Area, Totale di somministrazioni 
		 */
		Dataset<Row> dfSomministrazioni = Query1Processing.getSomministrazioni(spark);

		/*
		 * preprocessing somministrazioni summary -> ordinare temporalmente
		 */
		
		
		Dataset<Row> dfSomministrazioniCentri = Query1Processing.getJoinDf(dfCentri, dfSomministrazioni);
		dfSomministrazioniCentri.show();
		
		//JavaRDD<Tuple3<Date,String, Integer>> somministrazioni = Query1Processing.getSomministrazioni(sc);
        sc.stop();

	}

	public static String getFilePuntiTipologia() {
		return filePuntiTipologia;
	}


	public static String getFileSomministrazioneVaccini() {
		return fileSomministrazioneVaccini;
	}


}
