package com.harri.task2.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static com.harri.task2.utils.SparkConfig.getSession;
import static com.harri.task2.utils.Constants.*;

public class HiveQuery {
    public static Dataset<Row> topCountries() {
        Dataset<Row> topCountries = getSession().sql(
                "SELECT Country, SUM(Salary) AS SalaryIncomes " +
                "FROM allInfo " +
                "GROUP BY Country " +
                "ORDER BY SalaryIncomes DESC " +
                "LIMIT 3");

        topCountries.write().option("header", true)
                .mode("overwrite").format("csv")
                .save(TOP_COUNTRIES_LOCAL_PATH);
//                .save(TOP_COUNTRIES_S3_PATH);

        return topCountries;
    }

    public static Dataset<Row> topClubs() {
        Dataset<Row> topClubs = getSession().sql(
                "SELECT Club, SUM(Salary) AS SalarySpend " +
                        "FROM allInfo " +
                        "GROUP BY Club " +
                        "ORDER BY SalarySpend DESC " +
                        "LIMIT 5");

        topClubs.write().option("header", true)
                .mode("overwrite").format("csv")
                .save(TOP_CLUBS_LOCAL_PATH);
//                .save(TOP_CLUBS_S3_PATH);

        return topClubs;
    }

    public static Dataset<Row> bestContinents() {
        Dataset<Row> bestContinents = getSession().sql(
                "SELECT continent," +
                        " ROUND(AVG(CAST(`Fifa Score` AS INT)), 2) AS Score " +
                        "FROM allInfo " +
                        "WHERE (continent LIKE '%America%' OR continent LIKE 'Europe') AND CAST(`Fifa Score` AS INT) >= 80 " +
                        "GROUP BY continent " +
                        "ORDER BY Score DESC");


        bestContinents.write().option("header", true)
                .mode("overwrite").format("csv")
                .save(BEST_CONTINENTS_LOCAL_PATH);
//                .save(BEST_CONTINENTS_S3_PATH);

        return bestContinents;
    }
}
