package spark.customlists;

import scala.Tuple2;
import java.util.List;

public class CustomListInteger
{
    List<Tuple2<Integer, Integer>> list = null;



    public CustomListInteger(List<Tuple2<Integer, Integer>> thisList)
    {
        this.list = thisList;
    }

    @Override
    public String toString()
    {
        int i = 0;
        StringBuffer string = new StringBuffer();
        while(i < list.size())
        {
            Tuple2<Integer, Integer> tuple = list.get(i);
            string.append(String.valueOf(tuple._1) + " " + tuple._2 + "\n");
            i++;
        }
        return string.toString();
    }

}