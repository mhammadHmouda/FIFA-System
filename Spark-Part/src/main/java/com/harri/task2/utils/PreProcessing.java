package com.harri.task2.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import java.text.Normalizer;
import java.util.regex.Pattern;
import static com.harri.task2.utils.SparkConfig.getSession;
import static com.harri.task2.utils.Constants.*;
import static org.apache.spark.sql.functions.call_udf;
import static org.apache.spark.sql.functions.col;

public class PreProcessing {
    public static Dataset<Row> execute(Dataset<Row> allInfoDS) {
        // To transform salary and Value column to valid format as Float instead of String
        getSession().udf().register("filter",
                (String salary) -> {
                    String numericPart = salary.replaceAll("[^0-9.]", "");
                    if (salary.matches("€\\d*\\.?\\d+K"))
                        return Double.parseDouble(numericPart) * 1000;
                    else if (salary.matches("€\\d*\\.?\\d+M"))
                        return Double.parseDouble(numericPart) * 1000000;
                    else
                        return Double.parseDouble(numericPart);
                },
                DataTypes.DoubleType);


        getSession().udf().register("convertToEnglishUDF", (String input) -> convertToEnglish(input), DataTypes.StringType);

        allInfoDS = allInfoDS.withColumn("Salary", call_udf("filter", col("Salary")));
        allInfoDS = allInfoDS.withColumn("Value", call_udf("filter", col("Value")));
        allInfoDS = allInfoDS.withColumn("Country", call_udf("convertToEnglishUDF", col("Country")));
        allInfoDS = allInfoDS.withColumn("Name", call_udf("convertToEnglishUDF", col("Name")));
        allInfoDS = allInfoDS.filter(col("Club").isNotNull());

        allInfoDS.write().option("header", true)
                .mode("overwrite").format("csv")
                .save(PLAYERS_CLEANING_LOCAL_PATH);

        return allInfoDS;
    }

    public static String convertToEnglish(String input) {
        String normalized = Normalizer.normalize(input, Normalizer.Form.NFD);
        Pattern pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
        String result = pattern.matcher(normalized).replaceAll("");
        return result.replaceAll("[^\\x00-\\x7F]", "");
    }
}
