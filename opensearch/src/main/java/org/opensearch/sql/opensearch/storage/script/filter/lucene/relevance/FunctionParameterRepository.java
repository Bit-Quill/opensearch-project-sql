package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.SimpleQueryStringFlag;
import org.opensearch.index.query.support.QueryParsers;
import org.opensearch.index.search.MatchQuery;
import org.opensearch.sql.data.model.ExprValue;

public class FunctionParameterRepository {
  private FunctionParameterRepository() {
  }

  public static final Map<String, String> ArgumentLimitations;

  static {
    ArgumentLimitations = ImmutableMap.<String, String>builder()
        .put("boost", "Accepts only floating point values greater than 0.")
        .put("tie_breaker", "Accepts only floating point values in range 0 to 1.")
        .put("rewrite", "Available values are: constant_score, "
            + "scoring_boolean, constant_score_boolean, top_terms_X, top_terms_boost_X, "
            + "top_terms_blended_freqs_X, where X is an integer value.")
        .put("flags", String.format(
            "Available values are: %s and any combinations of these separated by '|'.",
            Arrays.stream(SimpleQueryStringFlag.class.getEnumConstants())
                .map(Enum::toString).collect(Collectors.joining(", "))))
        .put("time_zone", "For more information, follow this link: "
            + "https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html#of-java.lang.String-")
        .put("fuzziness", "Available values are: "
            + "'AUTO', 'AUTO:x,y' or z, where x, y, z - integer values.")
        .put("operator", String.format("Available values are: %s.",
            Arrays.stream(Operator.class.getEnumConstants())
                .map(Enum::toString).collect(Collectors.joining(", "))))
        .put("type", String.format("Available values are: %s.",
            Arrays.stream(MultiMatchQueryBuilder.Type.class.getEnumConstants())
                .map(Enum::toString).collect(Collectors.joining(", "))))
        .put("zero_terms_query", String.format("Available values are: %s.",
            Arrays.stream(MatchQuery.ZeroTermsQuery.class.getEnumConstants())
                .map(Enum::toString).collect(Collectors.joining(", "))))
        .put("int", "Accepts only integer values.")
        .put("float", "Accepts only floating point values.")
        .put("bool", "Accepts only boolean values: 'true' or 'false'.")
        .build();
  }

  private static String formatErrorMessage(String name, String value) {
    return formatErrorMessage(name, value, name);
  }

  private static String formatErrorMessage(String name, String value, String limitationName) {
    return String.format("Invalid %s value: '%s'. %s",
        name, value, ArgumentLimitations.containsKey(name) ? ArgumentLimitations.get(name)
            : ArgumentLimitations.getOrDefault(limitationName, ""));
  }

  /**
   * Check whether value is valid for 'rewrite' or 'fuzzy_rewrite'.
   * @param value Value
   * @param name Value name
   * @return Converted
   */
  public static String checkRewrite(ExprValue value, String name) {
    try {
      QueryParsers.parseRewriteMethod(
          value.stringValue().toLowerCase(), null, LoggingDeprecationHandler.INSTANCE);
      return value.stringValue();
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage(name, value.stringValue(), "rewrite"));
    }
  }

  /**
   * Convert ExprValue to Flags.
   * @param value Value
   * @return Array of flags
   */
  public static SimpleQueryStringFlag[] convertFlags(ExprValue value) {
    try {
      return Arrays.stream(value.stringValue().toUpperCase().split("\\|"))
          .map(SimpleQueryStringFlag::valueOf)
          .toArray(SimpleQueryStringFlag[]::new);
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage("flags", value.stringValue()), e);
    }
  }

  /**
   * Check whether ExprValue could be converted to timezone object.
   * @param value Value
   * @return Converted to string
   */
  public static String checkTimeZone(ExprValue value) {
    try {
      ZoneId.of(value.stringValue());
      return value.stringValue();
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage("time_zone", value.stringValue()), e);
    }
  }

  /**
   * Convert ExprValue to Fuzziness object.
   * @param value Value
   * @return Fuzziness
   */
  public static Fuzziness convertFuzziness(ExprValue value) {
    try {
      return Fuzziness.build(value.stringValue().toUpperCase());
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage("fuzziness", value.stringValue()), e);
    }
  }

  /**
   * Convert ExprValue to Operator object, could be used for 'operator' and 'default_operator'.
   * @param value Value
   * @param name Value name
   * @return Operator
   */
  public static Operator convertOperator(ExprValue value, String name) {
    try {
      return Operator.fromString(value.stringValue().toUpperCase());
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage(name, value.stringValue(), "operator"));
    }
  }

  /**
   * Convert ExprValue to Type object.
   * @param value Value
   * @return Type
   */
  public static MultiMatchQueryBuilder.Type convertType(ExprValue value) {
    try {
      return MultiMatchQueryBuilder.Type.parse(value.stringValue().toLowerCase(),
          LoggingDeprecationHandler.INSTANCE);
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage("type", value.stringValue()), e);
    }
  }

  /**
   * Convert ExprValue to ZeroTermsQuery object.
   * @param value Value
   * @return ZeroTermsQuery
   */
  public static MatchQuery.ZeroTermsQuery convertZeroTermsQuery(ExprValue value) {
    try {
      return MatchQuery.ZeroTermsQuery.valueOf(value.stringValue().toUpperCase());
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage("zero_terms_query", value.stringValue()), e);
    }
  }

  /**
   * Convert ExprValue to int.
   * @param value Value
   * @param name Value name
   * @return int
   */
  public static int convertIntValue(ExprValue value, String name) {
    try {
      return Integer.parseInt(value.stringValue());
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage(name, value.stringValue(), "int"), e);
    }
  }

  /**
   * Convert ExprValue to float.
   * @param value Value
   * @param name Value name
   * @return float
   */
  public static float convertFloatValue(ExprValue value, String name) {
    try {
      return Float.parseFloat(value.stringValue());
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage(name, value.stringValue(), "float"), e);
    }
  }

  /**
   * Convert ExprValue to bool.
   * @param value Value
   * @param name Value name
   * @return bool
   */
  public static boolean convertBoolValue(ExprValue value, String name) {
    try {
      // Boolean.parseBoolean interprets integers or any other stuff as a valid value
      Boolean res = Boolean.parseBoolean(value.stringValue());
      if (value.stringValue().equalsIgnoreCase(res.toString())) {
        return res;
      } else {
        throw new Exception("Invalid boolean value");
      }
    } catch (Exception e) {
      throw new RuntimeException(formatErrorMessage(name, value.stringValue(), "bool"), e);
    }
  }
}
