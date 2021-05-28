package utility;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Hdfs {
    private static Hdfs instance = null;

    private final String inputDir = "/input";
    private final String outputDir = "/output";
    private SparkSession sparkSession;
    private String hdfsUrl;
    private FileSystem hdfs;

    private Hdfs(SparkSession sparkSession, String hdfsUrl) {
        this.sparkSession = sparkSession;
        this.hdfsUrl = hdfsUrl;
        Configuration cnf = new Configuration();
        cnf.set("fs.defaultFS", hdfsUrl);
        try {
            this.hdfs = FileSystem.get(cnf);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void saveDataset(Dataset<Row> df, String filename) {
        df.write()
                .format("csv")
                .option("header", true)
                .mode(SaveMode.Overwrite)
                .save(this.hdfsUrl + outputDir + "/" + filename );
    }

    public Dataset<Row> getDatasetInput( String filename ){
        return this.sparkSession.read().csv( this.hdfsUrl + inputDir + "/" + filename);
    }


    public static Hdfs createInstance( SparkSession sparkSession, String hdfsUrl){
        if( instance == null ) instance = new Hdfs( sparkSession, hdfsUrl);
        return instance;
    }

    public void saveDurations(long duration1, long duration2, long duration3) {
        String[] head = {"query1", "query2", "query3"};
        String[] record = { String.valueOf(duration1), String.valueOf(duration2), String.valueOf(duration3)};
        List<String[]> data = new ArrayList<>();
        data.add(head);
        data.add(record);
        try (CSVWriter writer = new CSVWriter(new FileWriter(this.hdfsUrl + outputDir + "/time-queries.csv"))) {
            writer.writeAll(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
