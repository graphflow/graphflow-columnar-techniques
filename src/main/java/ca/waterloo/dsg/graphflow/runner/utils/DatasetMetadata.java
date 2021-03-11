package ca.waterloo.dsg.graphflow.runner.utils;

import ca.waterloo.dsg.graphflow.runner.utils.DatasetMetadata.RelFileDescription.CardinalityDeserializer;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog.Cardinality;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.stream.JsonReader;
import lombok.Getter;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DatasetMetadata {

    private static abstract class FileDescription {

        @Getter private String filename;
    }

    public static class NodeFileDescription extends FileDescription {

        @Getter private String type;
    }

    public static class RelFileDescription extends FileDescription {

        public static class CardinalityDeserializer implements JsonDeserializer<Cardinality> {

            @Override
            public Cardinality deserialize(JsonElement json, Type typeOfT,
                JsonDeserializationContext context) throws JsonParseException {
                var values = GraphCatalog.Cardinality.values();
                for (var value: values) {
                    if (value.val.equals(json.getAsString())) {
                        return value;
                    }
                }
                return null;
            }
        }

        @Getter private String label;
        @Getter private Cardinality cardinality;
    }

    public char separator;
    public NodeFileDescription[] nodeFileDescriptions;
    public RelFileDescription[] relFileDescriptions;

    public static DatasetMetadata readDatasetMetadata(String path) throws IOException {
        var GSONBuilder = new GsonBuilder();
        GSONBuilder.registerTypeAdapter(Cardinality.class, new CardinalityDeserializer());
        var JSONFileData = Files.readAllBytes(Paths.get(path + "metadata.json"));
        return GSONBuilder.create().fromJson(new JsonReader(new StringReader(new String(
            JSONFileData, StandardCharsets.US_ASCII))), DatasetMetadata.class);
    }
}
