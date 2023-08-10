package com.harri.task2;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.varia.NullAppender;
import org.apache.spark.sql.SparkSession;

public class SparkConfig {
    public static SparkSession getSession(){
        BasicConfigurator.configure(new NullAppender());

        return SparkSession.builder()
                .appName("FIFA-Spark")
                .master("local")
                .getOrCreate();
    }
}
