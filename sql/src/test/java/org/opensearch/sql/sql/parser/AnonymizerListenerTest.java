package org.opensearch.sql.sql.parser;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.sql.antlr.AnonymizerListener;

public class AnonymizerListenerTest {
  AnonymizerListener anonymizer = new AnonymizerListener();
  ErrorNode errorNode = new ErrorNode() {

    @Override
    public Interval getSourceInterval() {
      return null;
    }

    @Override
    public ParseTree getParent() {
      return null;
    }

    @Override
    public Object getPayload() {
      return null;
    }

    @Override
    public ParseTree getChild(int i) {
      return null;
    }

    @Override
    public int getChildCount() {
      return 0;
    }

    @Override
    public String toStringTree() {
      return null;
    }

    @Override
    public String toStringTree(Parser parser) {
      return null;
    }

    @Override
    public void setParent(RuleContext parent) {
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      return null;
    }

    @Override
    public String getText() {
      return null;
    }

    @Override
    public Token getSymbol() {
      return null;
    }
  };

  @Test
  public void visitErrorNodeTest() {
    anonymizer.visitErrorNode(errorNode);
  }
}
