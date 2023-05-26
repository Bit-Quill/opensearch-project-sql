package org.opensearch.sql.sql.antlr;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;

import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.*;

public class AnonymizerListener implements ParseTreeListener {
  private String anonymizedQueryString = "";
  private final int NO_TYPE = -1;
  private int previousType = NO_TYPE;

  @Override
  public void enterEveryRule(ParserRuleContext ctx) {
  }

  @Override
  public void exitEveryRule(ParserRuleContext ctx) {
  }

  @Override
  public void visitTerminal(TerminalNode node) {
    /*
    if ((node.getSymbol().getType() != RR_BRACKET
        && (node.getSymbol().getType() != LR_BRACKET || previousType == FROM || previousType == COMMA)
        && node.getSymbol().getType() != COMMA
        && node.getSymbol().getType() != DOT
        && previousType != LR_BRACKET
        && previousType != DOT
        && previousType != -1)
        && !((node.getSymbol().getType() == PLUS
            || node.getSymbol().getType() == MINUS
            || node.getSymbol().getType() == MULTIPLY
            || node.getSymbol().getType() == DIVIDE
            || node.getSymbol().getType() == EQUAL_SYMBOL
            || node.getSymbol().getType() == LESS_SYMBOL
            || node.getSymbol().getType() == GREATER_SYMBOL)
            && (previousType == PLUS
            || previousType == MINUS
            || previousType == MULTIPLY
            || previousType == DIVIDE
            || previousType == EQUAL_SYMBOL
            || previousType == LESS_SYMBOL
            || previousType == GREATER_SYMBOL))) {
      anonymizedQueryString += " ";
    }
     */

    if (node.getSymbol().getType() != DOT && previousType != DOT && node.getSymbol().getType() != COMMA) {
      anonymizedQueryString += " ";
    }

    switch (node.getSymbol().getType()) {
      case ID:
      case TIMESTAMP:
      case BACKTICK_QUOTE_ID:
        if (previousType == FROM) {
          anonymizedQueryString += "table";
        } else {
          anonymizedQueryString += "identifier";
        }
        break;
      case ZERO_DECIMAL:
      case ONE_DECIMAL:
      case TWO_DECIMAL:
      case DECIMAL_LITERAL:
      case REAL_LITERAL:
        anonymizedQueryString += "number";
        break;
      case STRING_LITERAL:
        anonymizedQueryString += "'string_literal'";
        break;
      case BOOLEAN:
      case TRUE:
      case FALSE:
        anonymizedQueryString += "boolean_literal";
        break;
      case NO_TYPE:
        // end of file
        break;
      default:
        anonymizedQueryString += node.getText().toUpperCase();
    }
    previousType = node.getSymbol().getType();
  }

  @Override
  public void visitErrorNode(ErrorNode node) {
  }

  public String getAnonymizedQueryString() {
    return "(" + anonymizedQueryString + ")";
  }
}
