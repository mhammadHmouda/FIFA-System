package com.harri.task2.app;

import com.harri.task2.utils.HiveQuery;
import com.harri.task2.utils.PlayersUtils;
import com.harri.task2.utils.PreProcessing;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import java.io.IOException;
import java.util.Scanner;
import static com.harri.task2.utils.SparkConfig.getSession;
import static com.harri.task2.utils.Constants.*;

public class Main {
    public static void main(String[] args) throws IOException {

        // Get players data from csv file
        Dataset<Row> playersDS = getSession().read()
                .option("header", true).csv(PLAYERS_PATH);

        Dataset<Row> continentCountries = PlayersUtils.loadContinentsFromApi(playersDS);


        Dataset<Row> allInfoDS = playersDS.join(continentCountries,
                playersDS.col("Nationality").equalTo(continentCountries.col("Country"))).drop("Nationality");

        allInfoDS = allInfoDS.persist(StorageLevel.MEMORY_AND_DISK());


        // (1-3)- Build file of players per continent
        PlayersUtils.buildFilePerContinent(allInfoDS);


        // ***********************************************************************


        // 4- Get the new updated document of players and extract the players with new salaries
        Dataset<Row> updatedSalary = getSession().read()
                .option("header", true)
                .csv(UPDATED_PLAYERS_PATH)
                .withColumnRenamed("Salary", "UpdatedSalary");

        // Create and merge new dataset contains the updated players salary
        PlayersUtils.updatePlayersDocument(playersDS, updatedSalary);


        // ***********************************************************************


        // Create some preprocessing of all info dataset such as reformat (Score and Value columns)
        allInfoDS = PreProcessing.execute(allInfoDS);


        // Create hive table contains all important information about players, continents and countries
        allInfoDS.createOrReplaceTempView("allInfo");

        // ***********************************************************************

        //1- Which top 3 countries that achieve the highest income through their players?
        HiveQuery.topCountries().show();


        // ***********************************************************************


        //2- List The club that includes the most valuable players, The top 5 clubs that spends highest salaries
        HiveQuery.topClubs().show();

        // ***********************************************************************


        //3- Which of Europe or America has the best FIFA players?
        HiveQuery.bestContinents().show();

        allInfoDS.unpersist();

        Scanner scanner = new Scanner(System.in);
        System.out.print("Finished");
        scanner.nextLine();

        getSession().stop();
    }
}