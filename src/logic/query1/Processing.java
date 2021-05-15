package logic.query1;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.spark.sql.functions.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import entity.CentriSomministrazione;
import main.Main;
import parser.CentriSomministrazioneParser;
import scala.Tuple2;
import scala.Tuple3;

public class Processing {
	
	public static Dataset<Row> parseCsvCentri( SparkSession spark ){
		
		Dataset<Row> df = spark.read()
				.csv(Main.getFilePuntiTipologia());
		
		df = df.withColumnRenamed("_c0", "area");
		df = df.withColumnRenamed("_c1", "denominazione_struttura");
		df = df.select( "area", "denominazione_struttura" );
	    return df;
	}
	
	public static Dataset<Row> parseCsvSomministrazioni( SparkSession spark ){
		
		Dataset<Row> df = spark.read()
				.csv(Main.getFileSomministrazioneVaccini());
		
		df = df.withColumnRenamed("_c0", "data");
		df = df.withColumnRenamed("_c1", "area");
		df = df.withColumnRenamed("_c2", "totale");
		df = df.select( "data", "area", "totale" );
		
	    return df;
	}
	
	public static Dataset<Row> getTotalCenters( SparkSession spark, Dataset<Row> df ){
		
		JavaRDD<String> input = df.select("area").javaRDD().map(row -> (String)row.get(0));
		
		// Transformations
        JavaRDD<String> areas = input.flatMap(line -> CentriSomministrazioneParser.getArea(line).iterator());        
        JavaPairRDD<String, Integer> pairs = areas.mapToPair(area -> new Tuple2<>(area, 1));
        JavaPairRDD<String, Integer> results = pairs.reduceByKey((x, y) -> x+y);
        Dataset<Row> dfResult = spark.createDataset( JavaPairRDD.toRDD(results), Encoders.tuple(Encoders.STRING(),Encoders.INT())).toDF();
        dfResult = dfResult.withColumnRenamed("_1", "area");
        dfResult = dfResult.withColumnRenamed("_2", "numeroCentri");
        return dfResult;
	}
	
	public static Dataset<Row> getJoinDf( Dataset<Row> dfCentri, Dataset<Row> dfSomministrazioni ){
		Dataset<Row> df = dfSomministrazioni.join( dfCentri, "area");
		df = df.sort("area", "data");
		return df;

	}
	
}
