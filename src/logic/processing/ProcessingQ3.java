package logic.processing;

import java.io.IOException;
import java.util.Date;
import java.util.Calendar;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

import main.Main;
import utility.LogController;

public class ProcessingQ3 {

    public static Dataset<Row> parseCsvTotalePopolazione(SparkSession spark){
        Dataset<Row> df = Main.getHdfs().getDatasetInput("totale-popolazione.csv");
        df = df.withColumnRenamed("_c0", "area");
        df = df.withColumnRenamed("_c1", "popolazione");

        df = df.select( "area", "popolazione" );
        return df;

    }

    public static Dataset<Row> parseCsvSomministrazioni( SparkSession spark ){
        // creo dataset con le colonne (data, area, totale) del file csv
        // successivamente ordino le entry per area e data
        Dataset<Row> df = Main.getHdfs().getDatasetInput("somministrazioni-vaccini-summary-latest.csv");
        df = df.withColumnRenamed("_c0", "data");
        df = df.withColumnRenamed("_c1", "area");
        df = df.withColumnRenamed("_c2", "totale");
        df = df.select( "data", "area", "totale" );
        df = df.sort("area", "data");
        return df;
    }

    public static Date getFilterDate() throws IOException {
        Calendar giugno2021 = Calendar.getInstance();
        giugno2021.set(Calendar.YEAR, 2021);
        giugno2021.set(Calendar.MONTH, Calendar.JUNE);
        giugno2021.set(Calendar.DAY_OF_MONTH, 1);
        return giugno2021.getTime();

    }

}
