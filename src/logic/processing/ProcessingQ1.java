package logic.processing;



import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import main.Main;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import parser.CentriSomministrazioneParser;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;


public class ProcessingQ1 {
	
	public static Dataset<Row> parseCsvCentri( SparkSession spark ){
		// creo dataset con le colonne (area, denominazione_struttra) del file csv
		Dataset<Row> df = Main.getHdfs().getDatasetInput("punti-somministrazione-tipologia.csv");
		df = df.withColumnRenamed("_c0", "area");
		df = df.withColumnRenamed("_c1", "denominazione_struttura");
		df = df.select( "area", "denominazione_struttura" );
	    return df;
	}
	
	public static Dataset<Row> parseCsvSomministrazioni( SparkSession spark ){
		// creo dataset con le colonne (data, area, totale) del file csv
		// successivamente ordino le entry per area e data
		Dataset<Row> df = Main.getHdfs().getDatasetInput("somministrazioni-vaccini-summary-latest.csv");

		df = df.withColumnRenamed("_c1", "data");
		df = df.withColumnRenamed("_c0", "area");
		df = df.withColumnRenamed("_c2", "totale");
		df = df.select( "data", "area", "totale" );
		df = df.sort("area", "data");
	    return df;
	}

	
	public static JavaPairRDD<String, Integer> getTotalCenters( Dataset<Row> df ){
		// dal primo file csv trovo il numero di centri vaccinali per ogni area
		JavaRDD<String> input = df.select("area").javaRDD().map(row -> (String)row.get(0));
		
		// Transformations
        JavaRDD<String> areas = input.flatMap(line -> CentriSomministrazioneParser.getArea(line).iterator());        
        JavaPairRDD<String, Integer> pairs = areas.mapToPair(area -> new Tuple2<>(area, 1));
        return pairs.reduceByKey((x, y) -> x+y);
        
	}



	
}
