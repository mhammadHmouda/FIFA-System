package com.harri.task2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class UpdateSalary {
    public static void update(Dataset<Row> playersDS, Dataset<Row> updatedSalary) {

        Dataset<Row> results = playersDS
                .join(updatedSalary, new String[]{"Name", "Value",  "Club"})
                .filter("Salary != UpdatedSalary")
                .drop("Salary")
                .withColumnRenamed("UpdatedSalary", "Salary");

        results.write()
                .option("header", true)
                .mode("overwrite")
                .format("csv")
                .save("s3a://fifa-players-results/updatedSalary");

    }
}
