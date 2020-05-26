package spark;

import java.io.File;
import java.sql.SQLException;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.Wrapper;
import org.apache.catalina.startup.Tomcat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import spark.rddoperations.SparkOperations;
import spark.servlets.*;

public class Server {

    public static void main(String[] args) throws LifecycleException {

        SparkConf conf = new SparkConf().setAppName("Project1").setMaster("local[8]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> inputFile = sparkContext.textFile("C:\\Users\\Garrison\\Project-1-Garrison\\src\\main\\resources\\scrubbed.csv", 8);
        
        SparkOperations SO = new SparkOperations(sparkContext,inputFile);

        // try {
        //     SO.runOperations();
        // } catch (SQLException e1) {
        //     e1.printStackTrace();
        // }

        System.out.println(SO.sightingsByDuration().sortByKey().collect());
  
        Tomcat tomcat = new Tomcat();
        tomcat.setBaseDir(new File("target/tomcat/").getAbsolutePath());
        tomcat.setPort(8080);
        tomcat.getConnector();
        tomcat.addWebapp("/spark", new File("src/main/").getAbsolutePath());
        Wrapper byTimeServlet = tomcat.addServlet("/spark", "ByTimeServlet", new ByTimeServlet());
        byTimeServlet.addMapping("/time");
        Wrapper byStateServlet = tomcat.addServlet("/spark", "ByStateServlet", new ByStateServlet(sparkContext,inputFile));
        byStateServlet.addMapping("/state");
        Wrapper byCountryServlet = tomcat.addServlet("/spark", "ByCountryServlet", new ByCountryServlet(sparkContext,inputFile));
        byCountryServlet.addMapping("/country");
        Wrapper byShapeServlet = tomcat.addServlet("/spark", "ByShapeServlet", new ByShapeServlet());
        byShapeServlet.addMapping("/shape");
        Wrapper byDurationServlet = tomcat.addServlet("/spark", "byDurationServlet", new ByDurationServlet());
        byDurationServlet.addMapping("/duration");

        tomcat.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    System.out.println("Shutting down tomcat");
                    tomcat.stop();
                    sparkContext.close();
                } catch (LifecycleException e) {
                    e.printStackTrace();
                }
            }
        });

        
   }
}