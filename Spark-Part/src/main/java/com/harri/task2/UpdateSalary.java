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

        results.show();

        results.write()
                .option("header", true)
                .mode("overwrite")
                .format("csv")
                .save("D:\\Harri\\Training1\\Java Spark Train\\version\\FIFA-System\\Spark-Part\\src\\main\\resources\\outputs\\updatedSalary");
//                .save("s3a://fifa-players-results/updatedSalary");

    }
}
