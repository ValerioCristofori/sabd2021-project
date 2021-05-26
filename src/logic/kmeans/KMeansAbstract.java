package logic.kmeans;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;

import java.io.Serializable;

public abstract class KMeansAbstract implements Serializable {

    // clusterizzo i dati in k classi usando KMeans
    public abstract void train (JavaRDD<Vector> dataset, Integer k, Integer iterations);
    public abstract Integer predict(Vector denseVector);
    public abstract Double getCost ();
    public abstract Double getWSSSE (JavaRDD<Vector> dataset);

}
