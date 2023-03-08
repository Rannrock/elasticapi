package org.example.elastico;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.example.elastic.Elastic;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {

    public static void main(String[] args) {

        System.out.println("hello world");

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[2]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> dataset = spark.read()
                .option("header", "true")
                .option("delimiter", ",")
                .csv("/home/citarf/Documents/data/12_million_plus_company_data.csv");

        dataset = dataset.withColumn("current_employee_estimate", functions.col("current_employee_estimate").cast(DataTypes.IntegerType));
        dataset = dataset.withColumn("total_employee_estimate", functions.col("total_employee_estimate").cast(DataTypes.IntegerType));

        dataset.printSchema();
        dataset.show();
        System.out.println(dataset.count());


        Elastic elastic = new Elastic("192.168.0.200", 9200);

        String indiceName = "company";

        try {
            elastic.createIndice(indiceName, dataset.schema());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        writeWithJson(dataset, elastic, indiceName);

        spark.close();
        System.out.println("FIN");
        System.exit(0);
    }

    public static void writeWithJson(Dataset<Row> dataset, Elastic elastic, String indiceName) {

        Path dataPath = Paths.get("/home/citarf/Documents/data");

        dataset.coalesce(1).write()
                .mode("overwrite")
                .option("ignoreNullFields", "false")
                .json(dataPath.resolve(indiceName + "_data").toString());

        String data = Stream.of(new File(dataPath.resolve(indiceName + "_data").toString()).listFiles())
                .filter(file -> !file.isDirectory())
                .filter(file -> file.getName().startsWith("part-00000-"))
                .map(File::getName)
                .findFirst().get();

        elastic.writeData(indiceName, dataPath.resolve(indiceName + "_data").resolve(data));
    }



}
