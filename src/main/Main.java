package main;

import java.io.IOException;
import java.lang.Double;
import java.lang.Float;
import java.lang.Long;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.*;


import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import entity.SommDonne;

import logic.*;
import logic.kmeans.KMeansAbstract;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.*;


import entity.Somministrazione;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.*;

import utility.LogController;

public class Main {

	private static String filePuntiTipologia = "data/punti-somministrazione-tipologia.csv";
	private static String fileSomministrazioneVaccini = "data/somministrazioni-vaccini-summary-latest.csv";
	private static String fileSomministrazioneVacciniDonne = "data/somministrazioni-vaccini-latest.csv";
	private static String fileTotalePopolazione = "data/totale-popolazione.csv";



	public static void main(String[] args) throws SecurityException, IOException, AnalysisException {
		
		//query1();
		query2();
		//query3();
	}


	private static void query1() throws SecurityException, IOException {
		// Q1: partendo dai file csv, per ogni mese e area, calcolare le somministrazioni medie giornaliere in un centro generico
		// vengono considerati i dati a partire dal 2021-01-01
		
		SparkConf conf = new SparkConf().setAppName("Query1");
		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			SparkSession spark = SparkSession
				    .builder()
				    .appName("Java Spark SQL Query1")
				    .getOrCreate();

			// prendere coppie (area, numero di centri)

			Dataset<Row> dfCentri = ProcessingQ1.parseCsvCentri(spark);
			JavaPairRDD<String, Integer> centriRdd = ProcessingQ1.getTotalCenters(dfCentri);

			// prendere triple (data, area, numero di somministrazioni)

			Dataset<Row> dfSomministrazioni = ProcessingQ1.parseCsvSomministrazioni(spark);

			// preprocessing somministrazioni summary -> ordinare temporalmente

			Dataset<Somministrazione> dfSomm = dfSomministrazioni.as( Encoders.bean(Somministrazione.class) );
			JavaRDD<Somministrazione> sommRdd = dfSomm.toJavaRDD()
					.filter( somm -> somm.getData().contains("2021")); // filtro solo per il 2021
			
			JavaPairRDD<Tuple2<String,String>,Tuple2<Integer,Integer>> process = sommRdd // process: <<area, mese> <somministrazioni, 1>>
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
	

	private static void query2() throws IOException, AnalysisException {
		// Q2: partendo dal file csv, per le donne e per ogni mese,
		// fare una classifica delle prime 5 aree per cui si prevede il maggior numero di somministrazioni il primo giorno del mese successivo
		// si considerano i dati di un mese per predire il mese successivo (partendo da gennaio 2021)
		// tutto questo per ogni mese, per ogni area, per ogni fascia
		// per la predizione si sfrutta la regressione lineare


		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat simpleMonthFormat = new SimpleDateFormat("MM");
		
		SparkConf conf = new SparkConf().setAppName("Query2");
		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			SparkSession spark = SparkSession
				    .builder()
				    .appName("Java Spark SQL Query2")
				    .getOrCreate();
			Date gennaioData = ProcessingQ2.getFilterDate();

			List<StructField> listfields = new ArrayList<>();
			listfields.add(DataTypes.createStructField("data", DataTypes.StringType, false));
			listfields.add(DataTypes.createStructField("fascia", DataTypes.StringType, false));
			listfields.add(DataTypes.createStructField("area", DataTypes.StringType, false));
			listfields.add(DataTypes.createStructField("somministrazioni_previste", DataTypes.IntegerType, false));
			StructType resultStruct = DataTypes.createStructType(listfields);
		
			Dataset<SommDonne> dfSommDonne = ProcessingQ2.parseCsvSommDonne(spark);
			JavaRDD<SommDonne> sommDonneRdd = dfSommDonne.toJavaRDD();
			JavaPairRDD<Tuple3<Date, String, String>, Integer > datiFiltrati = sommDonneRdd.mapToPair(
					somm -> new Tuple2<>(
								new Tuple3<>( simpleDateFormat.parse( somm.getData() ), somm.getArea(), somm.getFascia()),
								Integer.valueOf( somm.getTotale() )) // filtro a partire dal 2021-01-01
					).filter( row -> !row._1._1().before( gennaioData )) // sommo i valori di somministrazione di tutte le entry con stessa chiave (diversi tipi di vaccino)
					.reduceByKey( (tuple1,tuple2) -> tuple1+tuple2);

			//result.value = il valore predetto per il mese dopo result.key._1
			JavaPairRDD<Tuple3<String, String, String>, SimpleRegression> trainingData =
					datiFiltrati.mapToPair(
	                        row -> {
	                        	String month = simpleMonthFormat.format(row._1._1());
	                        	long epochTime = row._1._1().getTime();
	                        	double val = Integer.valueOf(row._2).doubleValue();
	                            SimpleRegression simpleRegression = new SimpleRegression();
	                            simpleRegression.addData((double) epochTime, val);
	                            //LogController.getSingletonInstance().saveMess( String.format("Chiave %s,%s,%s%nValore %f%n", row._1._2(),row._1._3(),simpleDateFormat.format(row._1._1()),val));
	                            return new Tuple2<>(new Tuple3<>( month, row._1._2(), row._1._3()), simpleRegression);
	                        }).reduceByKey( (tuple1, tuple2) -> {
									tuple1.append(tuple2);
									return tuple1;
									});


			// nella chiave: mese = mese successivo, valore = valore predetto
			JavaPairRDD<Tuple2<String, String>, Iterable<Tuple2<String, Integer>>> risultatoGruppato =
					trainingData.mapToPair(row ->{
	                    int monthInt = Integer.parseInt(row._1._1());
	                    int nextMonthInt = monthInt % 12 + 1;
	                    String nextMonthString = String.valueOf(nextMonthInt);
	                    String nextDay = getNextDayToPredict(nextMonthInt);
	                    long epochToPredict = simpleDateFormat.parse(nextDay).getTime();
	                    double predictedSomm = row._2.predict( (double)epochToPredict );
	                    //LogController.getSingletonInstance().saveMess(String.format("Predizione per chiave %s,%s,%s%nValore %f%n",nextMonthString,row._1._2(),row._1._3(),predictedSomm));
	                    return new Tuple2<>(new Tuple3<>(nextMonthString,row._1._3(),Integer.valueOf( (int) Math.round( predictedSomm ))), row._1._2() );
	                }).sortByKey(
	                		new Tuple3Comparator<>( Comparator.<String>naturalOrder(),
									               Comparator.<String>naturalOrder(),
									               Comparator.<Integer>naturalOrder()),
							false, 1).mapToPair(row -> new Tuple2<>( new Tuple2<>( row._1._1(), row._1._2()), new Tuple2<>(
					row._2,     // area
					row._1._3())  // totale somministrazioni
			)).groupByKey();

			JavaPairRDD<Tuple2<String, String>, ArrayList<Tuple2<String, Integer>>> rank = risultatoGruppato
					.mapToPair( giorno_fascia -> new Tuple2<>(
							giorno_fascia._1,
							Lists.newArrayList(Iterables.limit(giorno_fascia._2,5))
					)).sortByKey(
							new Tuple2Comparator<>( Comparator.<String>naturalOrder(),
									Comparator.<String>naturalOrder()),
							false, 1 );



/*

			JavaPairRDD<Tuple2<String, String>, Tuple2<String, Integer> > rank = sc.parallelizePairs(risultatoClassificato);



			Dataset<Row> dfResult = spark.createDataFrame( rank
					.map(row -> RowFactory.create( getNextDayToPredict(row._1._1()),
													row._1._2(),
													row._2._1,
													row._2._2)), resultStruct);
			dfResult.show(100);*/
			for( Tuple2<Tuple2<String,String>, ArrayList<Tuple2<String,Integer>>> resRow : rank.collect() )
				for( Tuple2<String,Integer> top5 : resRow._2)
					LogController.getSingletonInstance()
						.queryOutput(
								String.format("Giorno: %s", getNextDayToPredict(resRow._1._1)),
								String.format("Fascia: %s",resRow._1._2),
								String.format("Area: %s", top5._1),
								String.format("Totale: %s", top5._2.toString())

						);


		}
	}



	private static void query3() throws IOException {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String[] kMeansAlgo = {"KMeansCustom", "BisectingKMeansCustom"};
		final String pathPackage = KMeansAbstract.class.getPackage().getName();


		SparkConf conf = new SparkConf().setAppName("Query3");
		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			SparkSession spark = SparkSession
					.builder()
					.appName("Java Spark SQL Query3")
					.getOrCreate();


			/*
			 * prendere coppie (area, popolazione) da totale-popolazione
			 */
			Dataset<Row> dfPopolazione = ProcessingQ3.parseCsvTotalePopolazione(spark);
			JavaRDD<Row> totPopolazioneRdd = dfPopolazione.toJavaRDD();
			JavaPairRDD<String, Long> totalePopolazione = totPopolazioneRdd.mapToPair(
					row -> new Tuple2<>(row.getString(0), Long.valueOf(row.getString(1))));


			Dataset<Row> dfSomministrazioni = ProcessingQ3.parseCsvSomministrazioni(spark);
			Dataset<Somministrazione> dfSomm = dfSomministrazioni.as(Encoders.bean(Somministrazione.class));
			JavaRDD<Somministrazione> sommRdd = dfSomm.toJavaRDD();

			// prendere Regione, (data, vaccinazioni)
			// e devo filtrare per isBefore giugno 2021
			// giugno2021 lo faccio in processingQ3
			JavaPairRDD<String, Tuple2<Date, Long>> areaDateSomm = sommRdd.mapToPair(somm -> new Tuple2<>(somm.getArea(),
					new Tuple2<>(simpleDateFormat.parse(somm.getData()), Long.valueOf(somm.getTotale())) ) )
					.filter(row -> row._2._1.before(ProcessingQ3.getFilterDate()));



			// prendo (regione, regressione lineare)
			// faccio regressione per stimare vaccinazioni giornaliere per giugno
			JavaPairRDD<String, SimpleRegression> areaRegression = areaDateSomm.mapToPair(row -> {
				SimpleRegression simpleRegression = new SimpleRegression();
				simpleRegression.addData((double) (row._2._1.getTime()/1000), row._2._2);
				return new Tuple2<>(row._1, simpleRegression);
			}).reduceByKey((a, b) -> {
				a.append(b);
				return a;
			});


			JavaPairRDD<String, Long> regionVaccinationsPred = areaRegression.mapToPair(
					row -> {
						long epochToPredict = ProcessingQ3.getFilterDate().getTime();
						return new Tuple2<>(row._1, (long) row._2.predict((double) epochToPredict/1000));
					}).union(areaDateSomm.mapToPair(row -> new Tuple2<>(row._1, row._2._2)));



			JavaPairRDD<String, Long> sommaVaccinazioni = regionVaccinationsPred.reduceByKey((tuple1, tuple2) -> tuple1 + tuple2);

			JavaPairRDD<String, Double> percentualeVaccinati = sommaVaccinazioni
					.join(totalePopolazione).mapToPair(row -> new Tuple2<>(row._1, (double) row._2._1/row._2._2));


			JavaPairRDD<String, Vector> areaSommVettore = percentualeVaccinati
					.mapToPair(row -> new Tuple2<>(row._1, Vectors.dense(row._2)));

			JavaRDD<Vector> dataset = areaSommVettore.map(row -> row._2);

			for ( Tuple2<String, Vector> row : areaSommVettore.collect() ){
				LogController.getSingletonInstance().queryOutput(
						row._1,
						row._2.toString()
				);
			}

			// Raggruppare dati in K cluster da 2 a 5 attraverso K-Means
			// scelgo tra i due algoritmi

			// creo dataset per inserire colonne del resultStruct
			List< Tuple2<Tuple4<String,Integer,Double,Double>,JavaRDD<Tuple3<String,Double,Integer>>> > result = new ArrayList<>();

			for (int i = 0; i < kMeansAlgo.length; i++){
				try {
					Class<?> algoCustom = Class.forName( pathPackage + "." + kMeansAlgo[i]);

				for( int k=2; k<=5; k++){

						KMeansAbstract kMeans = (KMeansAbstract) algoCustom.getConstructor().newInstance();

						kMeans.train(dataset, Integer.valueOf(k), 20);
						JavaPairRDD<String, Integer> areaCluster = areaSommVettore.mapToPair(row ->
								new Tuple2<>(row._1, kMeans.predict(row._2)));
						// get trainCost , get wssse
						JavaPairRDD<String, Tuple2<Integer, Double>> areaClusterPerc = areaCluster
								.join(percentualeVaccinati);


						// su ogni riga di result metto i valori degli attributi
						Tuple4<String,Integer,Double,Double> algoCaratteristiche = new Tuple4<>( kMeansAlgo[i], k , kMeans.getCost(), kMeans.getWSSSE(dataset));

						JavaRDD<Tuple3<String,Double,Integer>> risultatoArea = areaClusterPerc
								.map(row -> new Tuple3<>( row._1, row._2._2, row._2._1));
						result.add( new Tuple2<>(algoCaratteristiche,risultatoArea) );
					}
				}catch(ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e){
					e.printStackTrace();
				}
			}

			for( Tuple2<Tuple4<String,Integer,Double,Double>,JavaRDD<Tuple3<String,Double,Integer>>> entry : result )
				for( Tuple3<String,Double,Integer> areaEntry : entry._2.collect())
					LogController.getSingletonInstance().queryOutput(
							entry._1._1(),entry._1._2().toString(),areaEntry._1(),areaEntry._2().toString(),areaEntry._3().toString(), entry._1._3().toString(), entry._1._4().toString()
					);



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

	public static String getFileTotalePopolazione() {
		return fileTotalePopolazione;
	}

	public static String getNextDayToPredict(int month){
		if(month<10){
			return "2021-0" + month + "-01";
		}
		return "2021-" + month + "-01";
	}

	public static String getNextDayToPredict(String month){
		if( Integer.valueOf(month)<10){
			return "2021-0" + month + "-01";
		}
		return "2021-" + month + "-01";
	}


}
