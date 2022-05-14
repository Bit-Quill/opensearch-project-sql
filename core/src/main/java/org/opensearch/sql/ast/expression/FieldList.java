package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ToString
@EqualsAndHashCode(callSuper = false)
public class FieldList extends UnresolvedExpression {
  private FieldList() {
    this.fields = List.of();
  }

  public FieldList(List<String> fields) {
    this.fields = fields;
  }

  @Getter
  private final List<String> fields;

  @Override
  public List<UnresolvedExpression> getChild() {
    return List.of();
  }

  // Equivalent to ['*']
  public static final FieldList AllFields = new FieldList();
}
