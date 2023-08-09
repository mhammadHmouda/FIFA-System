package com.harri.task2;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.varia.NullAppender;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class Main {


    public static void main(String[] args) {
        BasicConfigurator.configure(new NullAppender());

        SparkSession spark = SparkSession.builder()
                .appName("FIFA-Spark")
                .master("local")
                .getOrCreate();

        String jdbcUrl = "jdbc:mysql://localhost:3306/fifa?allowPublicKeyRetrieval=true";
        String user = "root";
        String password = "Hamood12344321m#";

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", user);
        connectionProperties.put("password", password);

        // Get countries data from database
        Dataset<Row> countriesDS = spark.read()
                .jdbc(jdbcUrl, "countries", connectionProperties)
                .withColumnRenamed("name", "c_name");

        // Get continent data from database
        Dataset<Row> continentsDS = spark.read()
                .jdbc(jdbcUrl, "continents", connectionProperties)
                .withColumnRenamed("name", "continent");

        // Get players data from csv file
        Dataset<Row> playersDS = spark.read()
                .option("header", true)
                .csv("D:\\Harri\\Training1\\FIFA-System\\Spark-Part\\src\\main\\resources\\players.csv");

        // Join all these datasets together
        Dataset<Row> allInfoDS = countriesDS.join(
                continentsDS,
                countriesDS.col("continent_code").equalTo(continentsDS.col("code"))
                ).drop(continentsDS.col("code"))
                .join(
                        playersDS,
                        playersDS.col("Nationality").equalTo(countriesDS.col("c_name"))
                ).select("Name", "Nationality", "Club", "Fifa Score", "Salary", "continent");

//        // (1-3)-
//        BuildFilePerContinent.build(allInfoDS);

        // 4- Get the new updated document of players and extract the players with new salaries
        Dataset<Row> updatedSalary = spark.read()
                .option("header", true)
                .csv("D:\\Harri\\Training1\\FIFA-System\\Spark-Part\\src\\main\\resources\\updatedSalary.csv")
                .withColumnRenamed("Salary", "UpdatedSalary")
                .drop("Nationality", "Fifa Score", "Age");

//        UpdateSalary.update(playersDS, updatedSalary);

        // To transform salary column to valid format as Float instead of String
        spark.udf().register("salary",
                (String salary) -> Float.parseFloat(salary.replace("€", "")
                        .replace("K", "000")), DataTypes.FloatType);

        // Create hive table contains all important information about players, continents and countries
        allInfoDS.withColumn("Salary", call_udf("salary", col("Salary")))
                .createOrReplaceTempView("allInfo");

        //1- Which top 3 countries that achieve the highest income through their players?
        Dataset<Row> topCountries = spark.sql(
                "SELECT Nationality AS Country, SUM(Salary) AS SalaryIncomes " +
                        "FROM allInfo " +
                        "GROUP BY Nationality " +
                        "ORDER BY SalaryIncomes DESC " +
                        "LIMIT 3");
        topCountries.show();
//                .write().option("header", true).mode("overwrite").format("csv");
//                .save("s3a://fifa-players-results/topCountries");

        //2- List The club that includes the most valuable players, The top 5 clubs that spends highest salaries
        Dataset<Row> topClubs = spark.sql(
                "SELECT Club, SUM(Salary) AS SalarySpend " +
                        "FROM allInfo " +
                        "GROUP BY Club " +
                        "ORDER BY SalarySpend DESC " +
                        "LIMIT 5");
        topClubs.show();
//                .write().option(
//                "header", true).mode("overwrite").format("csv");
//                .save("s3a://fifa-players-results/topClubs");

        //3- Which of Europe or America has the best FIFA players?
        Dataset<Row> bestPlayersContinent = spark.sql(
                        "SELECT continent," +
                        " CAST(ROUND(AVG(CAST(`Fifa Score` AS INT)), 0) AS INT) AS Score " +
                        "FROM allInfo " +
                        "WHERE continent LIKE '%America%' OR continent LIKE 'Europe'" +
                        "GROUP BY continent " +
                        "ORDER BY Score DESC");

        bestPlayersContinent.show();
//                .withColumn("Score", round(col("Score")).cast("Integer")).show();
//                .write().option("header", true).mode("overwrite").format("csv");
//                .save("s3a://fifa-players-results/bestContinent");


        spark.stop();
    }
}