package main;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


import com.clearspring.analytics.util.Lists;
import entity.PointLR;
import entity.SommDonne;
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
		SparkConf conf = new SparkConf().setAppName("Query2");
		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			SparkSession spark = SparkSession
				    .builder()
				    .appName("Java Spark SQL Query2")
				    .getOrCreate();
		
			Dataset<SommDonne> dfSommDonne = ProcessingQ2.parseCsvSommDonne(spark);
			dfSommDonne.show();
			JavaRDD<SommDonne> sommDonneRdd = dfSommDonne.toJavaRDD(); //forse filter
			JavaPairRDD<Tuple3<String, String, String>, Iterable<Tuple2<String, String>>> trainingData = sommDonneRdd.mapToPair(
					somm -> new Tuple2<>(
								new Tuple3<>( somm.getMese(), somm.getArea(), somm.getFascia()),
								new Tuple2<>( somm.getGiorno(), somm.getTotale()) )
					).groupByKey();
					//.foreachPartition();
			//result.value e' il valore predetto per il mese dopo result.kxey._1
			JavaPairRDD<Tuple3<String,String,String>, String> result = trainingData.mapToPair( training -> {
				LogController.getSingletonInstance().saveMess("qui");
				List<Tuple2<String, String>> trainingList = Lists.newArrayList(training._2);
				Dataset<Row> train = spark.createDataset(Lists.newArrayList(training._2),Encoders.tuple(Encoders.STRING(),Encoders.STRING())).toDF();

				LogController.getSingletonInstance().saveMess("qua");
				LinearRegression lr = new LinearRegression()
						.setMaxIter(10)
						.setRegParam(0.3)
						.setElasticNetParam(0.8);
				// Fit the model.
				LinearRegressionModel lrModel = lr.fit(train);

				// Print the coefficients and intercept for linear regression.
				String printCoefficient = "Coefficients: "
						+ lrModel.coefficients() + " Intercept: " + lrModel.intercept();

				// Summarize the model over the training set and print out some metrics.
				LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
				String printIterations = "numIterations: " + trainingSummary.totalIterations();
				String printHistory = "objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory());
				trainingSummary.residuals().show();
				String printRMSE = "RMSE: " + trainingSummary.rootMeanSquaredError();
				String printR2 = "r2: " + trainingSummary.r2();
				LogController.getSingletonInstance().queryOutput(
															printCoefficient,
															printIterations,
															printHistory,
															printRMSE,
															printR2
														);
				return null;
			});


						//Dataset<Row> train = spark.createDataFrame(Arrays.asList(tuple),Encoders.STRING()).toDF();
						//start MLlib linear regression




			/*for( Tuple2< Tuple3<String,String,String>, Tuple2<String, String> > resRow : result.collect() ) {
				LogController.getSingletonInstance()
						.queryOutput(
								String.format("Mese: %s", resRow._1._1()),
								String.format("Area: %s", resRow._1._2()),
								String.format("Fascia: %s",resRow._1._3()),
								String.format("Giorno: %s", resRow._2._1()),
								String.format("Totale: %s",resRow._2._2())
						);
			}*/
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
