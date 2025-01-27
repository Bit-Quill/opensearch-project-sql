package org.opensearch.sql.utils;

import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.InvalidModificationException;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.SemanticCheckException;

@UtilityClass
public class JsonUtils {
  /**
   * Checks if given JSON string can be parsed as valid JSON.
   *
   * @param jsonExprValue JSON string (e.g. "{\"hello\": \"world\"}").
   * @return true if the string can be parsed as valid JSON, else false.
   */
  public static ExprValue isValidJson(ExprValue jsonExprValue) {
    ObjectMapper objectMapper = new ObjectMapper();

    if (jsonExprValue.isNull() || jsonExprValue.isMissing()) {
      return ExprValueUtils.LITERAL_FALSE;
    }

    try {
      objectMapper.readTree(jsonExprValue.stringValue());
      return LITERAL_TRUE;
    } catch (JsonProcessingException e) {
      return LITERAL_FALSE;
    }
  }

  /** Converts a JSON encoded string to an Expression object. */
  public static ExprValue castJson(ExprValue json) {
    JsonNode jsonNode = jsonToNode(json);
    return processJsonNode(jsonNode);
  }

  public static ExprValue extractJson(ExprValue json, ExprValue path) {
    String jsonString = json.stringValue();
    String jsonPath = path.stringValue();

    try {
      Configuration config = Configuration.builder()
              .options(Option.AS_PATH_LIST)
              .build();
      List<String> resultPaths = JsonPath.using(config).parse(jsonString).read(jsonPath);

      List<ExprValue> elements = new LinkedList<>();

      for (String resultPath : resultPaths) {
        Object result = JsonPath.parse(jsonString).read(resultPath);
        String resultJsonString = new ObjectMapper().writeValueAsString(result);
        try {
          elements.add(processJsonNode(jsonStringToNode(resultJsonString)));
        } catch (SemanticCheckException e) {
          elements.add(new ExprStringValue(resultJsonString));
        }
      }

      if (elements.size() == 1) {
        return elements.get(0);
      } else {
        return new ExprCollectionValue(elements);
      }
    } catch (PathNotFoundException e) {
      return LITERAL_NULL;
    } catch (InvalidJsonException | JsonProcessingException e) {
      final String errorFormat = "JSON string '%s' is not valid. Error details: %s";
      throw new SemanticCheckException(String.format(errorFormat, json, e.getMessage()), e);
    }
  }

  private static JsonNode jsonToNode(ExprValue json) {
    return jsonStringToNode(json.stringValue());
  }

  private static JsonNode jsonStringToNode(String jsonString) {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonNode;
    try {
      jsonNode = objectMapper.readTree(jsonString);
    } catch (JsonProcessingException e) {
      final String errorFormat = "JSON string '%s' is not valid. Error details: %s";
      throw new SemanticCheckException(String.format(errorFormat, jsonString, e.getMessage()), e);
    }
    return jsonNode;
  }

  private static ExprValue processJsonNode(JsonNode jsonNode) {
    if (jsonNode.isFloatingPointNumber()) {
      return new ExprDoubleValue(jsonNode.asDouble());
    }
    if (jsonNode.isIntegralNumber()) {
      return new ExprIntegerValue(jsonNode.asLong());
    }
    if (jsonNode.isBoolean()) {
      return jsonNode.asBoolean() ? LITERAL_TRUE : LITERAL_FALSE;
    }
    if (jsonNode.isTextual()) {
      return new ExprStringValue(jsonNode.asText());
    }
    if (jsonNode.isArray()) {
      List<ExprValue> elements = new LinkedList<>();
      for (var iter = jsonNode.iterator(); iter.hasNext(); ) {
        jsonNode = iter.next();
        elements.add(processJsonNode(jsonNode));
      }
      return new ExprCollectionValue(elements);
    }
    if (jsonNode.isObject()) {
      Map<String, ExprValue> values = new LinkedHashMap<>();
      for (var iter = jsonNode.fields(); iter.hasNext(); ) {
        Map.Entry<String, JsonNode> entry = iter.next();
        values.put(entry.getKey(), processJsonNode(entry.getValue()));
      }
      return ExprTupleValue.fromExprValueMap(values);
    }

    // in all other cases, return null
    return LITERAL_NULL;
  }

  /** Converts a JSON encoded string to an Expression object. */
  public static ExprValue setJson(ExprValue json, ExprValue path, ExprValue valueToInsert) {

    String jsonUnquoted = StringUtils.unquoteText(json.stringValue());
    String pathUnquoted = StringUtils.unquoteText(path.stringValue());
    Object valueUnquoted = valueToInsert.value();
    Configuration conf = Configuration.defaultConfiguration()
            .addOptions(Option.SUPPRESS_EXCEPTIONS);
    try {
      JsonPath jsonPath = JsonPath.compile(pathUnquoted);
      DocumentContext docContext = JsonPath.using(conf).parse(jsonUnquoted);
        switch (docContext.read(jsonPath)) {
            case null -> {
                // Insert a new property
                recursiveCreate(docContext, pathUnquoted, valueUnquoted);
                return new ExprStringValue(docContext.jsonString());
            }
            case String ignored -> {
                //Override an existing property
                String updatedJson = docContext.set(pathUnquoted, valueUnquoted).jsonString();
                return new ExprStringValue(updatedJson);

            }
            case Iterable<?> ignored -> {
                // New element in the array.
                String updatedJson = docContext.add(pathUnquoted, valueUnquoted).jsonString();
                return new ExprStringValue(updatedJson);

            }
            default -> {
                return LITERAL_NULL;
            }
        }
    } catch (InvalidModificationException | InvalidJsonException | InvalidPathException | IllegalArgumentException | UnsupportedOperationException ex) {
      return LITERAL_NULL;
    }

  }

  /**
   * Helper method to handle recursive scenario.
   * @param docContext incoming Json in Java object form.
   * @param path path in String to perform insertion.
   * @param value value to be inserted with given path.
   */
  private static void recursiveCreate(DocumentContext docContext, String path, Object value) {
      final int pos = path.lastIndexOf('.');
      final String parent = path.substring(0, pos);
      final String current = path.substring(pos + 1);
      // Attempt to read the current path as it is, trigger the recursive in case of deep insert.
      if (docContext.read(parent) == null) {
        recursiveCreate(docContext, parent, new LinkedHashMap<>());
      }
      docContext.put(parent, current, value);
  }
}

