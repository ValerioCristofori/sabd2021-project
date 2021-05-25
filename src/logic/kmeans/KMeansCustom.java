package logic.kmeans;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;


public class KMeansCustom extends KMeansInterface{
    private KMeansModel kMeansModel;

    public KMeansCustom() {
        this.kMeansModel = null;
    }

    @Override
    public void train(JavaRDD<Vector> dataset, Integer k, Integer iterations) {
        this.kMeansModel = KMeans.train(dataset.rdd(), k, iterations);
    }

    @Override
    public Integer predict(Vector denseVector) {
        return this.kMeansModel.predict(denseVector);
    }

    @Override
    public Double getCost() {
        return this.kMeansModel.trainingCost();
    }

    @Override
    public Double getWSSSE(JavaRDD<Vector> dataset) {
        return this.kMeansModel.computeCost(dataset.rdd());
    }
}
