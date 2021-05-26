package logic;

import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;

public class Tuple2Comparator<T1, T2> implements Comparator<Tuple2<T1, T2>>, Serializable {

    private final Comparator<T1> comparatorT1;
    private final Comparator<T2> comparatorT2;


    public Tuple2Comparator(Comparator<T1> comparatorT1, Comparator<T2> comparatorT2) {
        this.comparatorT1 = comparatorT1;
        this.comparatorT2 = comparatorT2;
    }


    @Override
    public int compare(Tuple2<T1, T2> o1, Tuple2<T1, T2> o2) {
        int result = this.comparatorT1.compare(o1._1(), o2._1());
        if (result == 0) {
            result = comparatorT2.compare(o1._2(), o2._2());
        }
        return result;
    }

}