package main;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import entity.CentriSomministrazione;
import logic.processing.Query1Processing;
import parser.CentriSomministrazioneParser;
import scala.Tuple2;

public class Main {

	private static String filePuntiTipologia = "data/punti-somministrazione-tipologia.csv";
	private static String fileSomministrazioneVaccini = "data/somministrazioni-vaccini-summary-latest.csv";
			
	public static void main(String[] args) {
		
		query1();
	}

	private static void query1() {
		
		
		SparkConf conf = new SparkConf().setAppName("Query1");
		JavaSparkContext sc= new JavaSparkContext(conf);
		List<CentriSomministrazione> centri = Query1Processing.getTotalCenters(sc);
		for( CentriSomministrazione c : centri ) {
			System.out.println( c.getArea() + " ---- " + c.getNumCentri() );
		}
		
        sc.stop();

	}

	public static String getFilePuntiTipologia() {
		return filePuntiTipologia;
	}


	public static String getFileSomministrazioneVaccini() {
		return fileSomministrazioneVaccini;
	}


}
