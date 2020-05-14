package spark.CustomLists;

import scala.Tuple2;
import java.util.List;

public class CustomListString
{
    List<Tuple2<String, Integer>> list = null;



    public CustomListString(List<Tuple2<String, Integer>> thisList)
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
            Tuple2<String, Integer> tuple = list.get(i);
            string.append(String.valueOf(tuple._1) + " " + tuple._2 + "\n");
            i++;
        }
        return string.toString();
    }

}