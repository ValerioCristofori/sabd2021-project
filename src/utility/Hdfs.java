package utility;

import au.com.bytecode.opencsv.CSVWriter;
import jdk.internal.net.http.frame.DataFrame;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.BufferedWriter;
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
                .option("header", false)
                .mode(SaveMode.Overwrite)
                .save(this.hdfsUrl + outputDir + "/" + filename);
    }

    public Dataset<Row> getDatasetInput(String filename) {
        return this.sparkSession.read().csv(this.hdfsUrl + inputDir + "/" + filename);
    }

    public void saveDurations(long duration1, long duration2, long duration3) {
        List<StructField> listfields = new ArrayList<>();
        listfields.add(DataTypes.createStructField("query", DataTypes.StringType, false));
        listfields.add(DataTypes.createStructField("tempo_millisecondi", DataTypes.StringType, false));
        StructType resultStruct = DataTypes.createStructType(listfields);
        String[] head = {"query1", "query2", "query3"};
        String[] record = { String.valueOf(duration1), String.valueOf(duration2), String.valueOf(duration3)};
        List<Row> data = new ArrayList<>();
        data.add( RowFactory.create(head[0], record[0]));
        data.add( RowFactory.create(head[1], record[1]));
        data.add( RowFactory.create(head[2], record[2]));
        Dataset<Row> df = this.sparkSession.createDataFrame( data, resultStruct);

        df.write()
                .format("csv")
                .option("header", true)
                .mode(SaveMode.Overwrite)
                .save(this.hdfsUrl + outputDir + "/time-queries.csv");
    }


    public static Hdfs createInstance(SparkSession sparkSession, String hdfsUrl) {
        if (instance == null) instance = new Hdfs(sparkSession, hdfsUrl);
        return instance;
    }



}
