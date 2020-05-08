package spark;

import java.io.IOException;
import java.util.Arrays;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ProjectServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

   // JavaSparkContext sparkContext;
    Dataset<Row> df;
    public void init() throws ServletException {
		// SparkConf conf = new SparkConf().setAppName("Project1").setMaster("local[4]");
        // sparkContext = new JavaSparkContext(conf);
        
		SparkSession sparkSession = SparkSession.builder()
        .appName("covid-19")
        .master("local")
        .getOrCreate();

        StructType schema = new StructType()
            .add("Country_name","string")
            .add("Country_code","string")
            .add("Date","string")
            .add("Confirmed_deaths", "integer")
            .add("Confirmed_cases", "integer");
    
         df = sparkSession.read()
            .schema(schema)
            .option("header",true)
            .option("mode","DROPMALFORMED")
            .csv("C:\\Users\\Garrison\\Project-1-Garrison\\src\\main\\resources\\covid-19-total-confirmed-cases-vs-total-confirmed-deaths.csv");
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // JavaRDD<String> namesRDD = sparkContext.textFile("C:\\Users\\Garrison\\Project-1-Garrison\\src\\main\\resources\\covid-19-total-confirmed-cases-vs-total-confirmed-deaths.csv");
        // JavaRDD<String> namesRDD2 = namesRDD.filter((x) -> {return x.contains("Zimbabwe");});
        //  resp.getWriter().println(namesRDD2.take(5));

        df.select("Country_name","Date","Confirmed_cases","Confirmed_deaths").filter("country_name == 'Zimbabwe'").show();
        //resp.getWriter().println(df.select("Confirmed_cases").filter("country_name == 'Zimbabwe'").show());

    
    }
        
}