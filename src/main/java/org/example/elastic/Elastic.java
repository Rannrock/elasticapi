package org.example.elastic;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import jakarta.json.spi.JsonProvider;
import jakarta.json.stream.JsonParser;
import org.apache.http.HttpHost;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.client.RestClient;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.*;

public class Elastic {

    ElasticsearchClient client;

    public Elastic(String url, int port) {
        // create a low-level client
        RestClient restClient = RestClient.builder(
                new HttpHost(
                        Objects.requireNonNull(url),
                        port
                )
        ).build();

        // Create the transport layer with jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
              restClient,
              new JacksonJsonpMapper()
        );

        // Create an API client
        client = new ElasticsearchClient(transport);

    }

    private Map<String, Property> sparkSchemaToElasticMapping(StructType schema) {

        Map<String, Property> mapping = new HashMap<>();

        Arrays.stream(schema.fields()).forEach(field ->
                mapping.put(field.name(), this.createProperty(field.dataType()))
        );

        return mapping;
    }

    private Property createProperty(DataType type) {
        if (type.sameType(DataTypes.StringType)) {
            return new Property.Builder().text(builder -> builder).build();
        } else if (type.sameType(DataTypes.TimestampType)) {
            return new Property.Builder().date(builder -> builder).build();
        } else if (type.sameType(DataTypes.IntegerType)) {
            return new Property.Builder().integer(builder -> builder).build();
        } else if (type.sameType(DataTypes.DoubleType)) {
            return new Property.Builder().double_(builder -> builder).build();
        } else {
            throw new RuntimeException("Spark datatype not supported in elastic mapping.");
        }

    }

    public List<JsonData> readJson(Path path) {

        List<JsonData> result = new ArrayList<>();
        InputStream inputStream;
        try {
            inputStream = new FileInputStream(path.toFile());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found", e);
        }

        JsonpMapper jsonpMapper = client._transport().jsonpMapper();
        JsonProvider jsonProvider = jsonpMapper.jsonProvider();
        JsonParser parser = jsonProvider.createParser(inputStream);

        while (parser.hasNext()) {
            result.add(JsonData.from(parser, jsonpMapper));
        }

        return result;
    }

    public List<JsonData> readJsonByChunk(Path path, JsonParser parser, int chunkSize) {

        List<JsonData> result = new ArrayList<>();
        InputStream inputStream;
        try {
            inputStream = new FileInputStream(path.toFile());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found", e);
        }

        JsonpMapper jsonpMapper = client._transport().jsonpMapper();
        JsonProvider jsonProvider = jsonpMapper.jsonProvider();

        if (parser == null) {
            parser = jsonProvider.createParser(inputStream);
        }

        while (parser.hasNext()) {
            if (result.size() == chunkSize) break;
            result.add(JsonData.from(parser, jsonpMapper));
        }

        return result;
    }

    public void writeData(String indiceName, Path jsonFile) {

        List<JsonData> jsonDatas = this.readJson(jsonFile);

        BulkRequest.Builder builder = new BulkRequest.Builder();

        for (JsonData jsonData : jsonDatas) {
            builder.operations(op -> op
                    .index(idx -> idx
                            .index(indiceName)
                            .document(jsonData)
                    )
            );
        }

        BulkResponse result;
        try {
            result = client.bulk(builder.build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (result.errors()) {
            System.err.println("Bulk had errors");
            for (BulkResponseItem item: result.items()) {
                if (item.error() != null) {
                    System.err.println(item.error().reason());
                }
            }
        }
    }

    public void writeData(String indiceName, List<Row> data) {

        BulkRequest.Builder builder = new BulkRequest.Builder();

        for (Row row : data) {
            builder.operations(op -> op
                    .index(idx -> idx
                            .index(indiceName)
                            .document(row)
                    )
            );
        }

        BulkResponse result;
        try {
            result = client.bulk(builder.build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (result.errors()) {
            System.err.println("Bulk had errors");
            for (BulkResponseItem item: result.items()) {
                if (item.error() != null) {
                    System.err.println(item.error().reason());
                }
            }
        }
    }

    public void createIndice(String name, StructType schema) throws IOException {

        TypeMapping.Builder mapping = new TypeMapping.Builder().properties(
                sparkSchemaToElasticMapping(schema)
        );

        if (client.indices().exists(new ExistsRequest.Builder().index(name).build()).value()) {
            client.indices().delete(new DeleteIndexRequest.Builder().index(name).build());
        }

        CreateIndexResponse createIndexResponse = client.indices().create(
                new CreateIndexRequest.Builder()
                        .index(name)
                        .mappings(mapping.build())
                        .build()
        );

        if (!createIndexResponse.acknowledged()) {
            throw new RuntimeException("Index creation error.");
        }
    }


}
