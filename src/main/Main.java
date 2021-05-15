package main;

import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import entity.CentriSomministrazione;
import entity.Somministrazione;
import logic.query1.Processing;
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
		Dataset<Row> dfCentri = Processing.parseCsvCentri(spark);
		dfCentri = Processing.getTotalCenters(spark, dfCentri);
		/*
		 *  prendere triple -> Data, Area, Totale di somministrazioni 
		 */
		Dataset<Row> dfSomministrazioni = Processing.parseCsvSomministrazioni(spark);

		/*
		 * preprocessing somministrazioni summary -> ordinare temporalmente
		 */		
		Dataset<Row> dfSomministrazioniCentri = Processing.getJoinDf(dfCentri, dfSomministrazioni);
		dfSomministrazioniCentri.show();

		Dataset<Somministrazione> dfSomm = dfSomministrazioniCentri.as( Encoders.bean(Somministrazione.class));
		//dfSomm.show(false);
		//JavaRDD<Somministrazione> sommRdd = dfSomm.toJavaRDD()
		//		.filter( somm -> somm.getData().contains("2021")); // only for 2021
		
		
        sc.stop();

	}

	public static String getFilePuntiTipologia() {
		return filePuntiTipologia;
	}


	public static String getFileSomministrazioneVaccini() {
		return fileSomministrazioneVaccini;
	}


}
