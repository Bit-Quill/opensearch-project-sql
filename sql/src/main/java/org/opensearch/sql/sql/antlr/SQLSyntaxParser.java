/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.antlr;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.common.antlr.Parser;
import org.opensearch.sql.common.antlr.SyntaxAnalysisErrorListener;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;

/**
 * SQL syntax parser which encapsulates an ANTLR parser.
 */
public class SQLSyntaxParser implements Parser {
  private AnonymizerListener anonymizer;
  private SyntaxAnalysisErrorListener syntaxAnalysisErrorListener;
  private static final Logger LOG = LogManager.getLogger(SQLSyntaxParser.class);

  public SQLSyntaxParser() {
    this.anonymizer = new AnonymizerListener();
    this.syntaxAnalysisErrorListener = new SyntaxAnalysisErrorListener();
  }

  public SQLSyntaxParser(AnonymizerListener anonymizer,
                         SyntaxAnalysisErrorListener syntaxAnalysisErrorListener) {
    this.anonymizer = anonymizer;
    this.syntaxAnalysisErrorListener = syntaxAnalysisErrorListener;
  }

  /**
   * Parse a SQL query by ANTLR parser.
   * @param query   a SQL query
   * @return        parse tree root
   */
  @Override
  public ParseTree parse(String query) {
    OpenSearchSQLLexer lexer = new OpenSearchSQLLexer(new CaseInsensitiveCharStream(query));
    OpenSearchSQLParser parser = new OpenSearchSQLParser(new CommonTokenStream(lexer));
    parser.addErrorListener(syntaxAnalysisErrorListener);
    parser.addParseListener(anonymizer);

    ParseTree parseTree = parser.root();
    LOG.info("New Engine Request Query: {}", anonymizer.getAnonymizedQueryString());

    return parseTree;
  }
}
