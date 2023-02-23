/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
public class SubsequentPageRequestBuilderTest {

  @Mock
  private OpenSearchExprValueFactory exprValueFactory;

  private final OpenSearchRequest.IndexName indexName = new OpenSearchRequest.IndexName("test");
  private final String scrollId = "scroll";

  private SubsequentPageRequestBuilder requestBuilder;

  @BeforeEach
  void setup() {
    requestBuilder = new SubsequentPageRequestBuilder(indexName, scrollId, exprValueFactory);
  }

  @Test
  public void build() {
    assertEquals(
        new ContinueScrollRequest(scrollId, exprValueFactory),
        requestBuilder.build()
    );
  }

  @Test
  public void getIndexName() {
    assertEquals(indexName, requestBuilder.getIndexName());
  }
}
