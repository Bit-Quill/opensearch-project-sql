package org.opensearch.sql.sql.antlr;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;

import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.RR_BRACKET;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.LR_BRACKET;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.FROM;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.COMMA;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.DOT;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.ID;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.TIMESTAMP;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.BACKTICK_QUOTE_ID;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.ZERO_DECIMAL;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.ONE_DECIMAL;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.TWO_DECIMAL;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.DECIMAL_LITERAL;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.REAL_LITERAL;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer.STRING_LITERAL;

public class AnonymizerListener implements ParseTreeListener {
    private String anonymizedQueryString = "";
    private int previousType = -1;

    @Override
    public void enterEveryRule(ParserRuleContext ctx) {
    }

    @Override
    public void visitTerminal(TerminalNode node) {
        if (node.getSymbol().getType() != RR_BRACKET &&
                (node.getSymbol().getType() != LR_BRACKET || previousType == FROM) &&
                node.getSymbol().getType() != COMMA &&
                node.getSymbol().getType() != DOT &&
                previousType != LR_BRACKET &&
                previousType != DOT &&
                previousType != -1) {
            anonymizedQueryString += " ";
        }

        switch(node.getSymbol().getType()) {
            case ID:
            case TIMESTAMP:
            case BACKTICK_QUOTE_ID:
                anonymizedQueryString += "identifier";
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
            case -1:
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

    @Override
    public void exitEveryRule(ParserRuleContext ctx) {
    }

    public String getAnonymizedQueryString() {
        return "( " + anonymizedQueryString + ")";
    }
}