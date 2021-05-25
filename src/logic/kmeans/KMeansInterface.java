package logic.kmeans;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;

public abstract class KMeansInterface {
    // clusterizzo i dati in k classi usando KMeans
    public abstract void train (JavaRDD<Vector> dataset, Integer k, Integer iterations);
    public abstract Integer predict(Vector denseVector);
    public abstract Double getCost ();
    public abstract Double getWSSSE (JavaRDD<Vector> dataset);

}
