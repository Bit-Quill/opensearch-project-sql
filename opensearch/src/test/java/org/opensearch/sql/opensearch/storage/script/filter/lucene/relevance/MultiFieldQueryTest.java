package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import static org.hamcrest.Matchers.hasEntry;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.hamcrest.collection.IsMapContaining;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.config.ExpressionConfig;

class MultiFieldQueryTest {
  MultiFieldQuery query;
  private final DSL dsl = new ExpressionConfig().dsl(new ExpressionConfig().functionRepository());
  private final String testQueryName = "test_query";
  private final Map<String, RelevanceQuery.QueryBuilderStep> actionMap
      = ImmutableMap.of("paramA", (o, v) -> o);

  @BeforeEach
  public void setUp() {
    query = mock(MultiFieldQuery.class,
        Mockito.withSettings().useConstructor(testQueryName, actionMap)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
  }

  @Test
  void createQueryBuilderTest() {
    String sampleQuery = "sample query";
    String sampleField = "fieldA";
    float sampleValue = 34f;

    var fieldSpec = ImmutableMap.<String, ExprValue>builder().put(sampleField,
        ExprValueUtils.floatValue(sampleValue)).build();

    query.createQueryBuilder(dsl.namedArgument("fields",
        new LiteralExpression(ExprTupleValue.fromExprValueMap(fieldSpec))),
        dsl.namedArgument("query",
            new LiteralExpression(ExprValueUtils.stringValue(sampleQuery))));

    verify(query).createBuilder(argThat(
            (ArgumentMatcher<ImmutableMap<String, Float>>) map -> map.size() == 1
                && map.containsKey(sampleField) && map.containsValue(sampleValue)),
        eq(sampleQuery));
  }
}
