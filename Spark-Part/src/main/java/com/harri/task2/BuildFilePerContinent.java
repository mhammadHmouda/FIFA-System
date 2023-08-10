package com.harri.task2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class BuildFilePerContinent {
    public static void build(Dataset<Row> results){


        results.select("Name", "continent")
                .write()
                .option("header", true)
                .format("csv")
                .mode("overwrite")
                .partitionBy("continent");
//                .save("s3a://fifa-players-results/players-per-continent");

    }
}
