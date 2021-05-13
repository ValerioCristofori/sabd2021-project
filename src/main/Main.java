package main;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import parser.CentriSomministrazioneParser;
import scala.Tuple2;

public class Main {

	private static String filePuntiTipologia = "data/punti-somministrazione-tipologia.csv";
	private static String fileSomministrazioneVaccini = "data/somministrazioni-vaccini-summary-latest.csv";
			
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Query1");
		JavaSparkContext sc= new JavaSparkContext(conf);
		JavaRDD<String> input = sc.textFile(filePuntiTipologia);
		
		//JavaRDD<String> centriSomministrazione = input.map(line -> CentriSomministrazioneParser.parseLine(line));
		
		// Transformations
        JavaRDD<String> areas = input.flatMap(line -> CentriSomministrazioneParser.getArea(line).iterator());
        
        JavaPairRDD<String, Integer> pairs = areas.mapToPair(area -> new Tuple2<>(area, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((x, y) -> x+y);
        
        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
          System.out.println(tuple._1() + ": " + tuple._2());
        }
        sc.stop();
	}

}
