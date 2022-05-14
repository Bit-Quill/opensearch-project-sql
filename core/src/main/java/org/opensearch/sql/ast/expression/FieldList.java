package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

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

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitFieldList(this, context);
  }

  // Equivalent to ['*']
  public static final FieldList AllFields = new FieldList();
}
