package com.harri.task2;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.shaded.org.apache.http.HttpEntity;
import org.apache.hadoop.shaded.org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.hadoop.shaded.org.apache.http.client.methods.HttpPost;
import org.apache.hadoop.shaded.org.apache.http.entity.StringEntity;
import org.apache.hadoop.shaded.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.hadoop.shaded.org.apache.http.impl.client.HttpClients;
import org.apache.hadoop.shaded.org.apache.http.util.EntityUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

public class GetContinents {
    public static Dataset<Row> loadFromApi(Dataset<Row> playersDS) throws IOException {

        List<String> countriesName = playersDS.select("Nationality")
                .distinct().as(Encoders.STRING()).collectAsList();

        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost("https://fifa-players-f3c3763af6a2.herokuapp.com/continents");
        httpPost.setEntity(new StringEntity(new Gson().toJson(countriesName), "UTF-8"));

        httpPost.setHeader("Content-Type", "application/json");
        CloseableHttpResponse response = httpClient.execute(httpPost);

        List<ContinentCountry> countryInfo = getDataFromResponse(response);

        return SparkConfig.getSession()
                .createDataset(countryInfo, Encoders.bean(ContinentCountry.class))
                .toDF("continent", "Nationality");
    }

    private static List<ContinentCountry> getDataFromResponse(CloseableHttpResponse response) throws IOException {
        HttpEntity responseEntity = response.getEntity();
        String responseContent = EntityUtils.toString(responseEntity);
        Type listType = new TypeToken<List<ContinentCountry>>() {}.getType();
        return new Gson().fromJson(responseContent, listType);
    }
}
