package logic.kmeans;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;

public class KMeansCustom extends KMeansAbstract{
    // implementa i metodi della classe astratta
    private KMeansModel kMeansModel;

    // training del modello KMeans con specifici parametri di cluster e di iterazioni massime
    @Override
    public void train(JavaRDD<Vector> dataset, Integer k, Integer iterations) {
        this.kMeansModel = KMeans.train(dataset.rdd(), k, iterations);
    }

    // predice il cluster a cui appartiene un campione
    @Override
    public Integer predict(Vector denseVector) {
        return this.kMeansModel.predict(denseVector);
    }

    // valuta il clustering calcolando il costo di training
    @Override
    public Double getCost() {
        return this.kMeansModel.trainingCost();
    }

    // valuta il clustering calcolando il WSSSE (Within Set Sum of Squared Errors)
    @Override
    public Double getWSSSE(JavaRDD<Vector> dataset) {
        return this.kMeansModel.computeCost(dataset.rdd());
    }
}
