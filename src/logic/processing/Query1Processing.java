package logic.processing;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import entity.CentriSomministrazione;
import main.Main;
import parser.CentriSomministrazioneParser;
import scala.Tuple2;
import scala.Tuple3;

public class Query1Processing {

	/*	public static JavaRDD<Tuple3<String, String, Double>> preprocessDataset(JavaSparkContext sc) {


        JavaRDD<String> energyFile = sc.textFile(pathToFile);
        JavaRDD<Outlet> outlets =
                energyFile.map(
                        // line -> OutletParser.parseJson(line))         // JSON
                        line -> OutletParser.parseCSV(line))            // CSV
                        .filter(x -> x != null && x.getProperty().equals("1"));
        JavaRDD<Tuple3<String, String, Double>> result = outlets.map(x -> new Tuple3<String, String, Double>
                (x.getHouse_id(), x.getTimestamp(), Double.parseDouble(x.getValue())));


        return result;



    }*/
	
	public static List<CentriSomministrazione> getTotalCenters(JavaSparkContext sc){
		List<CentriSomministrazione> centri = new ArrayList<>();
		
		JavaRDD<String> input = sc.textFile(Main.getFilePuntiTipologia());
		
		//JavaRDD<String> centriSomministrazione = input.map(line -> CentriSomministrazioneParser.parseLine(line));
		
		// Transformations
        JavaRDD<String> areas = input.flatMap(line -> CentriSomministrazioneParser.getArea(line).iterator());
        
        JavaPairRDD<String, Integer> pairs = areas.mapToPair(area -> new Tuple2<>(area, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((x, y) -> x+y);
        
        List<Tuple2<String, Integer>> output = counts.collect();
        
        for (Tuple2<?,?> tuple : output) {
        	centri.add( new CentriSomministrazione( (String)tuple._1(), (Integer)tuple._2));
        }
        return centri;
	}
}
