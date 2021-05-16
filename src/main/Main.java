package main;

import java.io.IOException;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import entity.Somministrazione;

import logic.query1.Processing;

import scala.Tuple2;

import utility.LogController;

public class Main {

	private static String filePuntiTipologia = "data/punti-somministrazione-tipologia.csv";
	private static String fileSomministrazioneVaccini = "data/somministrazioni-vaccini-summary-latest.csv";
			
	public static void main(String[] args) throws SecurityException, IOException {
		
		query1();
	}

	private static void query1() throws SecurityException, IOException {
		
		
		SparkConf conf = new SparkConf().setAppName("Query1");
		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			SparkSession spark = SparkSession
				    .builder()
				    .appName("Java Spark SQL Query1")
				    .getOrCreate();
			
			/*
			 *  prendere coppie -> Area, Numero di centri
			 */
			Dataset<Row> dfCentri = Processing.parseCsvCentri(spark);
			JavaPairRDD<String, Integer> centriRdd = Processing.getTotalCenters(dfCentri);
			/*
			 *  prendere triple -> Data, Area, Totale di somministrazioni 
			 */
			Dataset<Row> dfSomministrazioni = Processing.parseCsvSomministrazioni(spark);

			/*
			 * preprocessing somministrazioni summary -> ordinare temporalmente
			 */		
			Dataset<Somministrazione> dfSomm = dfSomministrazioni.as( Encoders.bean(Somministrazione.class) );
			JavaRDD<Somministrazione> sommRdd = dfSomm.toJavaRDD()
					.filter( somm -> somm.getData().contains("2021")); // only for 2021
			
			JavaPairRDD<Tuple2<String,String>,Tuple2<Integer,Integer>> process = sommRdd
					.mapToPair(somm -> new Tuple2<>(new Tuple2<>(somm.getArea(),somm.getMese()), new Tuple2<>( Integer.valueOf(somm.getTotale()), 1 ) ) )
					.reduceByKey( (tuple1, tuple2) -> new Tuple2<>( (tuple1._1 + tuple2._1), (tuple1._2 + tuple2._2) ));
			JavaPairRDD<Tuple2<String,String>, Float> avgRdd = process.mapToPair( tuple -> {
				Tuple2<Integer,Integer> val = tuple._2;
				Integer totale = val._1;
				Integer count = val._2;
				Tuple2<Tuple2<String,String>, Float> avg = new Tuple2<>( tuple._1, Float.valueOf( (float)totale/count) );
				return avg;
			});
			JavaPairRDD<String, Tuple2<String, Float> > res = avgRdd.mapToPair( row -> new Tuple2<>(row._1._1, new Tuple2<>(row._1._2, row._2) ) )
					.join( centriRdd )
					.mapToPair( tuple -> {
						Float avg = tuple._2._1._2;
						Integer numCentri = tuple._2._2;
						return new Tuple2<>( tuple._1, new Tuple2<>( tuple._2._1._1, Float.valueOf( avg.floatValue()/numCentri.floatValue() ) ) );
					});
			
			
			for( Tuple2<String, Tuple2<String, Float> > resRow : res.collect() ) {
				LogController.getSingletonInstance().saveMess( String.format( "Area: %s%nMese: %s%nAvg: %f%n", resRow._1, resRow._2._1, resRow._2._2) );
			}
			sc.stop();
		}

	}

	public static String getFilePuntiTipologia() {
		return filePuntiTipologia;
	}


	public static String getFileSomministrazioneVaccini() {
		return fileSomministrazioneVaccini;
	}


}
