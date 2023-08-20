package com.harri.task2.utils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.harri.task2.models.ContinentCountry;
import org.apache.hadoop.shaded.org.apache.http.HttpEntity;
import org.apache.hadoop.shaded.org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.hadoop.shaded.org.apache.http.client.methods.HttpPost;
import org.apache.hadoop.shaded.org.apache.http.entity.StringEntity;
import org.apache.hadoop.shaded.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.hadoop.shaded.org.apache.http.impl.client.HttpClients;
import org.apache.hadoop.shaded.org.apache.http.util.EntityUtils;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import static com.harri.task2.utils.Constants.*;

public class PlayersUtils {
    public static void buildFilePerContinent(Dataset<Row> results){
        results.select("Name", "continent")
                .write()
                .option("header", true)
                .format("csv")
                .mode("overwrite")
                .partitionBy("continent")
                .save(FILE_PER_CONTINENT_LOCAL_PATH);
//                .save(FILE_PER_CONTINENT_S3_PATH);
    }

    public static Dataset<Row> loadContinentsFromApi(Dataset<Row> playersDS) {

        playersDS = playersDS.repartition(4);

        return playersDS.select("Nationality").distinct()
                .mapPartitions((MapPartitionsFunction<Row, ContinentCountry>) countryPartition -> {

                    List<ContinentCountry> countryInfo = new ArrayList<>();

                    CloseableHttpClient httpClient = HttpClients.createDefault();
                    HttpPost httpPost = new HttpPost(CONTINENTS_API_URL);
                    httpPost.setHeader("Content-Type", "application/json");

                    List<String> nationalities = new ArrayList<>();
                    while (countryPartition.hasNext()) {
                        nationalities.add(countryPartition.next().getString(0));
                    }

                    httpPost.setEntity(new StringEntity(new Gson().toJson(nationalities), "UTF-8"));

                    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                        countryInfo.addAll(getDataFromResponse(response));
                    } catch (IOException e) {
                        System.out.println(e.getMessage());
                    }
                    return countryInfo.iterator();

                }, Encoders.bean(ContinentCountry.class)).toDF();
    }

    private static List<ContinentCountry> getDataFromResponse(CloseableHttpResponse response) throws IOException {
        HttpEntity responseEntity = response.getEntity();
        String responseContent = EntityUtils.toString(responseEntity);
        Type listType = new TypeToken<List<ContinentCountry>>() {}.getType();
        return new Gson().fromJson(responseContent, listType);
    }

    public static void updatePlayersDocument(Dataset<Row> playersDS, Dataset<Row> updatedSalary) {

        Dataset<Row> newPlayersDocument = playersDS
                .join(updatedSalary, new String[]{"Name", "Value",  "Club", "Nationality", "Fifa Score", "Age"}, "right");

        newPlayersDocument
                .drop("Salary").withColumnRenamed("UpdatedSalary", "Salary")
                .write().option("header", true).mode("overwrite").format("csv")
                .save(UPDATED_DOCUMENT_LOCAL_PATH);

        Dataset<Row> updatedPlayersOnly = newPlayersDocument
                .filter("Salary != UpdatedSalary")
                .drop("Salary")
                .withColumnRenamed("UpdatedSalary", "Salary");

        updatedPlayersOnly.show();

        updatedPlayersOnly.write()
                .option("header", true)
                .mode(SaveMode.Overwrite)
                .format("csv")
                .save(UPDATED_PLAYERS_LOCAL_PATH);
//                .save(UPDATED_PLAYERS_S3_PATH);

    }
}
