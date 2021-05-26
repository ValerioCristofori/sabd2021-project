package logic.kmeans;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.linalg.Vector;

public class BisectingKMeansCustom extends KMeansAbstract {
    private BisectingKMeansModel bisectingKMeansModel;

    public BisectingKMeansCustom() {
        this.bisectingKMeansModel = null;
    }

    @Override
    public void train(JavaRDD<Vector> dataset, Integer k, Integer iterations) {
        BisectingKMeans bisectingKMeans = new BisectingKMeans();
        bisectingKMeans.setK(k);
        this.bisectingKMeansModel = bisectingKMeans.run(dataset);
    }

    @Override
    public Integer predict(Vector denseVector) {
        return this.bisectingKMeansModel.predict(denseVector);
    }

    @Override
    public Double getCost() {
        return this.bisectingKMeansModel.trainingCost();
    }

    @Override
    public Double getWSSSE(JavaRDD<Vector> dataset) {
        return this.bisectingKMeansModel.computeCost(dataset);
    }
}
