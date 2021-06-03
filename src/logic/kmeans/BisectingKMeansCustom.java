package logic.kmeans;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.linalg.Vector;

public class BisectingKMeansCustom extends KMeansAbstract {
    // implementa i metodi della classe astratta
    private BisectingKMeansModel bisectingKMeansModel;

    // training del modello KMeans con specifici parametri di cluster e di iterazioni massime
    @Override
    public void train(JavaRDD<Vector> dataset, Integer k, Integer iterations) {
        BisectingKMeans bisectingKMeans = new BisectingKMeans();
        bisectingKMeans.setK(k);
        this.bisectingKMeansModel = bisectingKMeans.run(dataset);
    }

    // predice il cluster a cui appartiene un campione
    @Override
    public Integer predict(Vector denseVector) {
        return this.bisectingKMeansModel.predict(denseVector);
    }

    // valuta il clustering calcolando il costo di training
    @Override
    public Double getCost() {
        return this.bisectingKMeansModel.trainingCost();
    }

    // valuta il clustering calcolando il WSSSE (Within Set Sum of Squared Errors)
    @Override
    public Double getWSSSE(JavaRDD<Vector> dataset) {
        return this.bisectingKMeansModel.computeCost(dataset);
    }
}
