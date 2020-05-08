package spark;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ProjectServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    JavaSparkContext sparkContext;

    public void init() throws ServletException {
		SparkConf conf = new SparkConf().setAppName("Project1").setMaster("local");
        sparkContext = new JavaSparkContext(conf);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        JavaRDD<String> namesRDD = sparkContext.textFile("C:\\Users\\Garrison\\Project-1-Garrison\\src\\main\\resources\\covid-19-total-confirmed-cases-vs-total-confirmed-deaths.csv");
      //  JavaRDD<String> namesRDD2 = namesRDD.map((x) -> {return x.split(",");});
        resp.getWriter().println((namesRDD.take(2)));
    
    }
        
}