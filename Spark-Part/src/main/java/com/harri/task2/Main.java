package com.harri.task2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import java.io.IOException;
import static com.harri.task2.SparkConfig.getSession;
import static org.apache.spark.sql.functions.*;

public class Main {

    public static void main(String[] args) throws IOException {
        // Get players data from csv file
        Dataset<Row> playersDS = getSession().read()
                .option("header", true)
                .csv("D:\\Fifa-data\\players.csv");

        Dataset<Row> continentCountries = GetContinents.loadFromApi(playersDS);

        Dataset<Row> allInfoDS = continentCountries.join(playersDS, "Nationality")
                .select("Name", "Nationality", "Club", "Fifa Score", "Salary", "continent");


        // (1-3)- Build file of players per continent
        BuildFilePerContinent.build(allInfoDS);

        // ***********************************************************************


        // 4- Get the new updated document of players and extract the players with new salaries
        Dataset<Row> updatedSalary = getSession().read()
                .option("header", true)
                .csv("D:\\Fifa-data\\updatedSalary.csv")
                .withColumnRenamed("Salary", "UpdatedSalary")
                .drop("Nationality", "Fifa Score", "Age");

        // Create and merge new dataset contains the updated players salary
        UpdateSalary.update(playersDS, updatedSalary);

        // ***********************************************************************


        // To transform salary column to valid format as Float instead of String
        getSession().udf().register("salary",
                (String salary) -> Float.parseFloat(salary.replace("€", "")
                        .replace("K", "000")), DataTypes.FloatType);

        // Create hive table contains all important information about players, continents and countries
        allInfoDS.withColumn("Salary", call_udf("salary", col("Salary")))
                .createOrReplaceTempView("allInfo");

        // ***********************************************************************


        //1- Which top 3 countries that achieve the highest income through their players?
        Dataset<Row> topCountries = getSession().sql(
                "SELECT Nationality AS Country, SUM(Salary) AS SalaryIncomes " +
                        "FROM allInfo " +
                        "GROUP BY Nationality " +
                        "ORDER BY SalaryIncomes DESC " +
                        "LIMIT 3");
        topCountries.show();

        topCountries.write().option("header", true)
                .mode("overwrite").format("csv")
                .save("D:\\Harri\\Training1\\Java Spark Train\\version\\FIFA-System\\Spark-Part\\src\\main\\resources\\outputs\\topCountries");
//                .save("s3a://fifa-players-results/topCountries");

        // ***********************************************************************


        //2- List The club that includes the most valuable players, The top 5 clubs that spends highest salaries
        Dataset<Row> topClubs = getSession().sql(
                "SELECT Club, SUM(Salary) AS SalarySpend " +
                        "FROM allInfo " +
                        "GROUP BY Club " +
                        "ORDER BY SalarySpend DESC " +
                        "LIMIT 5");
        topClubs.show();

        topClubs.write().option("header", true)
                .mode("overwrite").format("csv")
                .save("D:\\Harri\\Training1\\Java Spark Train\\version\\FIFA-System\\Spark-Part\\src\\main\\resources\\outputs\\topClubs");
//                .save("s3a://fifa-players-results/topClubs");

        // ***********************************************************************


        //3- Which of Europe or America has the best FIFA players?
        Dataset<Row> bestPlayersContinent = getSession().sql(
                        "SELECT continent," +
                        " CAST(ROUND(AVG(CAST(`Fifa Score` AS INT)), 0) AS INT) AS Score " +
                        "FROM allInfo " +
                        "WHERE continent LIKE '%America%' OR continent LIKE 'Europe'" +
                        "GROUP BY continent " +
                        "ORDER BY Score DESC");

        bestPlayersContinent.show();

        bestPlayersContinent.write().option("header", true)
                .mode("overwrite").format("csv")
                .save("D:\\Harri\\Training1\\Java Spark Train\\version\\FIFA-System\\Spark-Part\\src\\main\\resources\\outputs\\bestContinent");
//                .save("s3a://fifa-players-results/bestContinent");

        // ***********************************************************************

        getSession().stop();
    }
}