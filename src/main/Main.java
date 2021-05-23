package main;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;


import com.clearspring.analytics.util.Lists;
import entity.PointLR;
import entity.SommDonne;

import logic.TupleComparator;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import entity.Somministrazione;
import logic.ProcessingQ1;
import logic.ProcessingQ2;
import scala.Tuple2;

import scala.Tuple3;
import scala.Tuple4;
import utility.LogController;

import javax.xml.crypto.Data;

public class Main {

	private static String filePuntiTipologia = "data/punti-somministrazione-tipologia.csv";
	private static String fileSomministrazioneVaccini = "data/somministrazioni-vaccini-summary-latest.csv";
	private static String fileSomministrazioneVacciniDonne = "data/somministrazioni-vaccini-latest.csv";

			
	public static void main(String[] args) throws SecurityException, IOException {
		
		//query1();
		query2();
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
			Dataset<Row> dfCentri = ProcessingQ1.parseCsvCentri(spark);
			JavaPairRDD<String, Integer> centriRdd = ProcessingQ1.getTotalCenters(dfCentri);
			/*
			 *  prendere triple -> Data, Area, Totale di somministrazioni 
			 */
			Dataset<Row> dfSomministrazioni = ProcessingQ1.parseCsvSomministrazioni(spark);

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
				LogController.getSingletonInstance()
						.queryOutput(
								String.format("Area: %s", resRow._1),
								String.format("Mese: %s",resRow._2._1),
								String.format("Avg: %f", resRow._2._2)
						);
			}
			sc.stop();
		}

	}
	

	private static void query2() throws IOException {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat simpleMonthFormat = new SimpleDateFormat("MM");
		
		SparkConf conf = new SparkConf().setAppName("Query2");
		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			SparkSession spark = SparkSession
				    .builder()
				    .appName("Java Spark SQL Query2")
				    .getOrCreate();
			Date gennaioData = ProcessingQ2.getFilterDate();
		
			Dataset<SommDonne> dfSommDonne = ProcessingQ2.parseCsvSommDonne(spark);
			JavaRDD<SommDonne> sommDonneRdd = dfSommDonne.toJavaRDD();
			JavaPairRDD<Tuple3<Date, String, String>, Integer > datiFiltrati = sommDonneRdd.mapToPair(
					somm -> new Tuple2<>(
								new Tuple3<>( simpleDateFormat.parse( somm.getData() ), somm.getArea(), somm.getFascia()),
								Integer.valueOf( somm.getTotale() )) //filtraggio per data a partire dal 01-01-2021
					).filter( row -> !row._1._1().before( gennaioData )) //sommo i valori di somministrazione di tutte le entry con stessa chiave ( diversi tipi di vaccino )
					.reduceByKey( (tuple1,tuple2) -> tuple1+tuple2);

			//result.value e' il valore predetto per il mese dopo result.kxey._1
			JavaPairRDD<Tuple3<String, String, String>, SimpleRegression> trainingData =
					datiFiltrati.mapToPair(
	                        row -> {
	                        	String month = simpleMonthFormat.format(row._1._1());
	                        	long epochTime = row._1._1().getTime();
	                        	double val = Integer.valueOf(row._2).doubleValue();
	                            SimpleRegression simpleRegression = new SimpleRegression();
	                            simpleRegression.addData((double) epochTime, val);
	                            LogController.getSingletonInstance().saveMess( String.format("Chiave %s,%s,%s%nValore %f%n", row._1._2(),row._1._3(),simpleDateFormat.format(row._1._1()),val));
	                            return new Tuple2<>(new Tuple3<>( month, row._1._2(), row._1._3()), simpleRegression);
	                        }).reduceByKey( (tuple1, tuple2) -> {
									tuple1.append(tuple2);
									return tuple1;
									});


			// nella chiave il mese e il mese successivo e il valore e il valore predetto
			JavaPairRDD<Tuple3<String,String,Integer>, String > result =
					trainingData.mapToPair(row ->{
	                    int monthInt = Integer.parseInt(row._1._1());
	                    int nextMonthInt = monthInt % 12 + 1;
	                    String nextMonthString = String.valueOf(nextMonthInt);
	                    String nextDay;
	                    if(nextMonthInt<10){
							nextDay = "2021-0" + nextMonthInt + "-01";
	                    } else {
							nextDay = "2021-" + nextMonthInt + "-01";
	                    }
	                    LogController.getSingletonInstance().saveMess( "String to predict" + nextDay );
	                    long epochToPredict = simpleDateFormat.parse(nextDay).getTime();
	                    double predictedSomm = row._2.predict( (double)epochToPredict );
	                    LogController.getSingletonInstance().saveMess(String.format("Predizione per chiave %s,%s,%s%nValore %f%n",nextMonthString,row._1._2(),row._1._3(),predictedSomm));
	                    return new Tuple2<>(new Tuple3<>(nextMonthString,row._1._3(),Integer.valueOf( (int) Math.round( predictedSomm ))), row._1._2() );
	                }).sortByKey(
	                		new TupleComparator<>( Comparator.<String>naturalOrder(),
									               Comparator.<String>naturalOrder(),
									               Comparator.<Integer>naturalOrder()),
							false, 1);




			for( Tuple2< Tuple3<String,String, Integer>, String> resRow : result.collect() ) {
				LogController.getSingletonInstance()
						.queryOutput(
								String.format("Mese: %s", resRow._1._1()),
								String.format("Area: %s", resRow._2),
								String.format("Fascia: %s",resRow._1._2()),
								String.format("Totale: %s",resRow._1._3())
						);
			}

		}
	}

	public static String getFilePuntiTipologia() {
		return filePuntiTipologia;
	}


	public static String getFileSomministrazioneVaccini() {
		return fileSomministrazioneVaccini;
	}


	public static String getFileSomministrazioneVacciniDonne() {
		return fileSomministrazioneVacciniDonne;
	}


}
