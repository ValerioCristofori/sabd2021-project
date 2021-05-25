package logic;

import java.util.Date;
import java.util.Calendar;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

import main.Main;

public class ProcessingQ3 {

    public static Dataset<Row> parseCsvTotalePopolazione(SparkSession spark){
        Dataset<Row> df = spark.read()
                .csv(Main.getFileTotalePopolazione());

        df = df.withColumnRenamed("_c0", "area");
        df = df.withColumnRenamed("_c1", "popolazione");

        df = df.select( "area", "popolazione" );
        return df;

    }

    public static Date getFilterDate() {
        // boh, necessaria???
        Calendar giugno2021 = Calendar.getInstance();
        giugno2021.set(Calendar.YEAR, 2021);
        giugno2021.set(Calendar.MONTH, Calendar.JUNE);
        giugno2021.set(Calendar.DAY_OF_MONTH, 1);
        return giugno2021.getTime();

    }

    public static Dataset<Row> parseCsvSomministrazioni(SparkSession spark) {

        return null;
    }
}
