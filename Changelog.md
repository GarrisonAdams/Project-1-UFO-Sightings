# Changelog

## Version 0.7.0
- byDurationServlet and byShapeServlet added
- created a postgres database on an ec2 instance on aws
- spark.database is a new package containing DatabaseConnector.java and DatabaseOperations.java
    DatabaseConnector.java creates the connection to the database
    DatabaseOperations.java includes methods that let one write data to a table, read from a table, and print out a database
- SparkOperations.java is responsible for using RDD operations on the database and writing the results (when applicable) to a database
- got rid of CustomListInteger.java

## Version 0.6.0
- Placed the servlets in spark/servlets
- Created CustomListInteger.java and CustomListString.java inside spark/CustomLists.
    - These were created to act as wrappers for the List<Tuple2<>> objects created by the RDD collect() operation. They make the output look better
- More refactoring. Streamlining everything.

## Version 0.5.0
- Included ByCountryServlet, ByStateServlet, ByTimeServlet
- created RDDCustomOperations, which includes RDD operations I have used over and over

## Version 0.4.0
- Program can now calculate the percentage increase/decrease in sightings between two years
- Program can now return the number of cases from any state or country


## Version 0.3.0
- Official csv file is scrubbed.csv, which describes UFO sightings in North America
- HTTP GET method now displays the locations of every UFO sighting in California
- Created rddFiltering() method and rddSeoncdsBetween() method

## Version 0.2.0
- Created ProjectServlet.java. It is the start of my project
- Included covid-19.csv file
- Updated pom.xml

## Version 0.1.0
- Tomcat and Apache Spark work
