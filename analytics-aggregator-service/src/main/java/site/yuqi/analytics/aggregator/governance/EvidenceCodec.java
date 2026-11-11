package site.yuqi.analytics.aggregator.governance;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.TreeSet;

/** This version's manifest uses only strings, booleans, nulls and integer numbers. */
public final class EvidenceCodec {
    private static final ObjectMapper JSON = new ObjectMapper();
    private EvidenceCodec() {}

    public static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    public static String canonical(Object value) {
        try {
            return JSON.writeValueAsString(sorted(JSON.valueToTree(value)));
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Cannot encode evidence", exception);
        }
    }

    public static JsonNode parse(String value) {
        try {
            return JSON.readTree(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Invalid evidence JSON", exception);
        }
    }

    private static JsonNode sorted(JsonNode node) {
        if (node.isObject()) {
            ObjectNode result = JSON.createObjectNode();
            TreeSet<String> names = new TreeSet<>();
            node.fieldNames().forEachRemaining(names::add);
            names.forEach(name -> result.set(name, sorted(node.get(name))));
            return result;
        }
        if (node.isArray()) {
            ArrayNode result = JSON.createArrayNode();
            node.forEach(value -> result.add(sorted(value)));
            return result;
        }
        return node;
    }
}
