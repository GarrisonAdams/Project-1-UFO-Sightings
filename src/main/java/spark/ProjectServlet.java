package spark;

import java.io.IOException;
import java.util.Arrays;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


// import org.apache.spark.sql.Dataset;
// import org.apache.spark.sql.Row;
// import org.apache.spark.sql.SparkSession;
// import org.apache.spark.sql.functions;
// import org.apache.spark.sql.types.StructType;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ProjectServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    JavaSparkContext sparkContext;

    public void init() throws ServletException {
		SparkConf conf = new SparkConf().setAppName("Project1").setMaster("local[8]");
        sparkContext = new JavaSparkContext(conf);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        JavaRDD<String> namesRDD = sparkContext.textFile("C:\\Users\\Garrison\\Project-1-Garrison\\src\\main\\resources\\scrubbed.csv",4);
        namesRDD.cache();

        JavaRDD<String> names2RDD = namesRDD.filter(x -> 
            {String[] splitRow = x.split(",");
            return splitRow[2].equals("ca");});

        JavaRDD<String> names3RDD = names2RDD.map(x -> {
            String[] splitRow = x.split(",");
            String result = splitRow[1] + "," + splitRow[2] + "," + splitRow[3];
            return result;
        });

        resp.getWriter().println(names3RDD.take(50));
        resp.getWriter().println(names3RDD.count());

        // String row = "one,two,three,four,five";
        // String[] splitRow = splitRow(row);
        // String splitRow123 = splitRow[1] + "," + splitRow[2] + "," + splitRow[3];
        // String combinedRow = combineSplitRow(splitRow);


        // resp.getWriter().println(row);
        // resp.getWriter().println(splitRow123);
        // resp.getWriter().println(combinedRow);

       
    
    }
    public String[] splitRow(String row)
    {
        return row.split(",");
    }

    public String combineSplitRow(String[] splitRow)
    {
        return String.join(",",splitRow);
    }
}