package spark;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Wrapper;
import org.apache.catalina.startup.Tomcat;

public class Server {
    
    public static void main(String[] args) throws LifecycleException {

        SparkConf conf = new SparkConf().setAppName("Project1").setMaster("local[8]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        Tomcat tomcat = new Tomcat();
        tomcat.setBaseDir(new File("target/tomcat/").getAbsolutePath());
        tomcat.setPort(8080);
        tomcat.getConnector();
        tomcat.addWebapp("/spark", new File("src/main/").getAbsolutePath());
        Wrapper projectServlet = tomcat.addServlet("/spark", "ProjectServlet", new ProjectServlet(sparkContext));
        projectServlet.addMapping("/project");
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