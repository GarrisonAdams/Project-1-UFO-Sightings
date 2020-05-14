package spark;

import java.io.File;
//import java.sql.SQLException;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.Wrapper;
import org.apache.catalina.startup.Tomcat;

//import spark.database.DatabaseOperations;
import spark.servlets.*;

public class Server {

    public static void main(String[] args) throws LifecycleException {

    
        // try {
        //     SparkOperations SO = new SparkOperations();
        //     SO.runOperations();
        // } catch (SQLException e) {
        //     e.printStackTrace();
        // }

        Tomcat tomcat = new Tomcat();
        tomcat.setBaseDir(new File("target/tomcat/").getAbsolutePath());
        tomcat.setPort(8080);
        tomcat.getConnector();
        tomcat.addWebapp("/spark", new File("src/main/").getAbsolutePath());
        // Wrapper projectServlet = tomcat.addServlet("/spark", "ProjectServlet", new ProjectServlet(sparkContext,rdd));
        // projectServlet.addMapping("/project");
        // Wrapper byTimeServlet = tomcat.addServlet("/spark", "ByTimeServlet", new ByTimeServlet(sparkContext,rdd));
        // byTimeServlet.addMapping("/time");
        // Wrapper byStateServlet = tomcat.addServlet("/spark", "ByStateServlet", new ByStateServlet(sparkContext,rdd));
        // byStateServlet.addMapping("/state");
        Wrapper byCountryServlet = tomcat.addServlet("/spark", "ByCountryServlet", new ByCountryServlet());
        byCountryServlet.addMapping("/country");
       

        tomcat.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    System.out.println("Shutting down tomcat");
                    tomcat.stop();
                } catch (LifecycleException e) {
                    e.printStackTrace();
                }
            }
        });

        
   }
}