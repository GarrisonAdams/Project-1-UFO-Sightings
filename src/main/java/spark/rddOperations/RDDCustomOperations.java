package spark.rddoperations;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class RDDCustomOperations
{
    public static JavaRDD<String> rddFiltering(JavaRDD<String> rdd, String text, int column) {
        
        JavaRDD<String> rdd2 = rdd.filter(x -> {
            String[] splitRow = x.split(",");
  
            return splitRow[column].equals(text);
        });
        return rdd2;
    }

    public static JavaRDD<String> rddStripToColumn(JavaRDD<String> rdd, int column)
    {
        JavaRDD<String> rdd2 = rdd.map(x ->
        {
            String[] splitRow = x.split(",");
            return splitRow[column];
        });
        return rdd2;
    }
   
    public static JavaPairRDD<String, Integer> rddCounterString(JavaRDD<String> rdd) {
        JavaPairRDD<String, Integer> rdd2 = rdd.mapToPair((x) -> new Tuple2<>(x, 1));
        return rdd2.reduceByKey((x, y) -> (x + y));
    }

    public static JavaPairRDD<Integer, Integer> rddCounterInteger(JavaRDD<Integer> rdd3) {
        JavaPairRDD<Integer, Integer> rdd2 = rdd3.mapToPair(x -> new Tuple2<>(x, 1));
        return rdd2.reduceByKey((x, y) -> (x + y));
    }
}