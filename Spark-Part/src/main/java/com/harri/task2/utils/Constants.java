package com.harri.task2.utils;

public class Constants {
    public static final String PLAYERS_PATH = "D:/Harri/Training1/Java Spark Train/FIFA-System/Spark-Part/src/main/resources/data/players.csv";
    public static final String UPDATED_PLAYERS_PATH = "D:/Harri/Training1/Java Spark Train/FIFA-System/Spark-Part/src/main/resources/data/updatedSalary.csv";
//    private static final String BASE_URL = "https://fifa-players-f3c3763af6a2.herokuapp.com";
    private static final String BASE_URL = "http://localhost:8080";
    public static final String CONTINENTS_API_URL = BASE_URL + "/continents";
    private static final String LOCAL_OUTPUT_PATH = "D:/Harri/Training1/Java Spark Train/FIFA-System/Spark-Part/src/main/resources/outputs";
    private static final String S3_BUCKET_PATH = "s3a://fifa-players-results";
    public static final String FILE_PER_CONTINENT_LOCAL_PATH = LOCAL_OUTPUT_PATH + "/filePerContinent";
    public static final String TOP_COUNTRIES_LOCAL_PATH = LOCAL_OUTPUT_PATH + "/topCountries";
    public static final String TOP_CLUBS_LOCAL_PATH = LOCAL_OUTPUT_PATH + "/topClubs";
    public static final String BEST_CONTINENTS_LOCAL_PATH = LOCAL_OUTPUT_PATH + "/bestContinent";
    public static final String UPDATED_PLAYERS_LOCAL_PATH = LOCAL_OUTPUT_PATH + "/updatedPlayers";
    public static final String FILE_PER_CONTINENT_S3_PATH = S3_BUCKET_PATH + "/filePerContinent";
    public static final String TOP_COUNTRIES_S3_PATH = S3_BUCKET_PATH + "/topCountries";
    public static final String TOP_CLUBS_S3_PATH = S3_BUCKET_PATH + "/topClubs";
    public static final String BEST_CONTINENTS_S3_PATH = S3_BUCKET_PATH + "/bestContinent";
    public static final String UPDATED_PLAYERS_S3_PATH = S3_BUCKET_PATH + "/updatedPlayers";
    public static final String UPDATED_DOCUMENT_LOCAL_PATH = LOCAL_OUTPUT_PATH + "/updatedDocument";
    public static final String PLAYERS_CLEANING_LOCAL_PATH = LOCAL_OUTPUT_PATH + "/playersCleaning";



}
