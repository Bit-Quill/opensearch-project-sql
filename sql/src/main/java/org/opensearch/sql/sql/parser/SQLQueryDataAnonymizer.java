/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.parser;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import lombok.Generated;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.*;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.*;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.planner.logical.*;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class to mask sensitive information in incoming PPL queries.
 */
public class SQLQueryDataAnonymizer extends AbstractNodeVisitor<String, String> {

    private static final String MASK_LITERAL = "identifier";

    /**
     * This method is used to anonymize sensitive data in PPL query.
     * Sensitive data includes user data.,
     *
     * @return ppl query string with all user data replace with "***"
     */
    public String anonymizeData(UnresolvedPlan plan) {
        return plan.accept(this, null);
    }

    public String anonymizeStatement(Statement plan) {
        return plan.accept(this, null);
    }

    /**
     * Handle Query Statement.
     */
    @Override
    public String visitQuery(Query node, String context) {
        return node.getPlan().accept(this, null);
    }

    @Override
    public String visitExplain(Explain node, String context) {
        return node.getStatement().accept(this, null);
    }

    @Override
    public String visitRelation(Relation node, String context) {
        return "FROM table";
    }

    @Override
    @Generated //To exclude from jacoco..will remove https://github.com/opensearch-project/sql/issues/1019
    public String visitTableFunction(TableFunction node, String context) {
        //<TODO>
        return null;
    }

    @Override
    public String visitFilter(Filter node, String context) {
        String child = node.getChild().get(0).accept(this, context);
        String condition = visitExpression(node.getCondition());
        return StringUtils.format("%s WHERE %s", child, condition);
    }

    /**
     * Build {@link LogicalRename}.
     */
    @Override
    public String visitRename(Rename node, String context) {
        String child = node.getChild().get(0).accept(this, context);
        ImmutableMap.Builder<String, String> renameMapBuilder = new ImmutableMap.Builder<>();
        for (Map renameMap : node.getRenameList()) {
            renameMapBuilder.put(visitExpression(renameMap.getOrigin()),
                    ((Field) renameMap.getTarget()).getField().toString());
        }
        String renames =
                renameMapBuilder.build().entrySet().stream().map(entry -> StringUtils.format("%s as %s",
                        entry.getKey(), entry.getValue())).collect(Collectors.joining(","));
        return StringUtils.format("%s | rename %s", child, renames);
    }

    /**
     * Build {@link LogicalAggregation}.
     */
    @Override
    public String visitAggregation(Aggregation node, String context) {
        String child = node.getChild().get(0).accept(this, context);
        //final String group = visitExpressionList(node.getGroupExprList());
        return StringUtils.format("%s", child//,
                //String.join(" ", visitExpressionList(node.getAggExprList()), groupBy(group)).trim()
        );
    }

    /**
     * Build {@link LogicalRareTopN}.
     */
    @Override
    public String visitRareTopN(RareTopN node, String context) {
        final String child = node.getChild().get(0).accept(this, context);
        List<Argument> options = node.getNoOfResults();
        Integer noOfResults = (Integer) options.get(0).getValue().getValue();
        String fields = visitFieldList(node.getFields());
        String group = visitExpressionList(node.getGroupExprList());
        return StringUtils.format("%s | %s %d %s", child,
                node.getCommandType().name().toLowerCase(),
                noOfResults,
                String.join(" ", fields, groupBy(group)).trim()
        );
    }

    /**
     * Build {@link LogicalProject} or {@link LogicalRemove} from {@link Field}.
     */
    @Override
    public String visitProject(Project node, String context) {
        String fields = visitExpressionList(node.getProjectList());
        String child = node.getChild().get(0).accept(this, context);

        return StringUtils.format("( SELECT %s %s )", fields, child);
    }

    /**
     * Build {@link LogicalSort}.
     */
    @Override
    public String visitSort(Sort node, String context) {
        String child = node.getChild().get(0).accept(this, context);
        // the first options is {"count": "integer"}
        String sortList = visitFieldList(node.getSortList());
        return StringUtils.format("%s ORDER BY %s", child, sortList);
    }

    private String visitFieldList(List<Field> fieldList) {
        return fieldList.stream().map(this::visitExpression).collect(Collectors.joining(","));
    }

    private String visitExpressionList(List<UnresolvedExpression> expressionList) {
        return expressionList.isEmpty() ? "" :
                expressionList.stream().map(this::visitExpression).collect(Collectors.joining(","));
    }

    private String visitExpression(UnresolvedExpression expression) {
        return expression.accept(this, null);
    }

    private String groupBy(String groupBy) {
        return Strings.isNullOrEmpty(groupBy) ? "" : StringUtils.format("BY %s", groupBy);
    }

    public String analyze(UnresolvedExpression unresolved, String context) {
        return unresolved.accept(this, context);
    }

    @Override
    public String visitLiteral(Literal node, String context) {
        switch (node.getType()) {
            case STRING:
                return "\'string_literal\'";
            case BOOLEAN:
                return "boolean_literal";
            case INTEGER:
            case LONG:
            case SHORT:
            case FLOAT:
            case DOUBLE:
                return "number";
        }

        return "identifier";
    }

    @Override
    public String visitInterval(Interval node, String context) {
        String value = node.getValue().accept(this, context);
        String unit = node.getUnit().name();
        return StringUtils.format("INTERVAL %s %s", value, unit);
    }

    @Override
    public String visitAnd(And node, String context) {
        String left = node.getLeft().accept(this, context);
        String right = node.getRight().accept(this, context);
        return StringUtils.format("%s and %s", left, right);
    }

    @Override
    public String visitOr(Or node, String context) {
        String left = node.getLeft().accept(this, context);
        String right = node.getRight().accept(this, context);
        return StringUtils.format("%s or %s", left, right);
    }

    @Override
    public String visitXor(Xor node, String context) {
        String left = node.getLeft().accept(this, context);
        String right = node.getRight().accept(this, context);
        return StringUtils.format("%s xor %s", left, right);
    }

    @Override
    public String visitNot(Not node, String context) {
        String expr = node.getExpression().accept(this, context);
        return StringUtils.format("not %s", expr);
    }

    @Override
    public String visitAggregateFunction(AggregateFunction node, String context) {
        String arg = node.getField().accept(this, context);
        return StringUtils.format("%s(%s)", node.getFuncName(), arg);
    }

    @Override
    public String visitFunction(Function node, String context) {
        List<String> arguments =
                node.getFuncArgs().stream()
                        .map(unresolvedExpression -> analyze(unresolvedExpression, context)).collect(Collectors.toList());
        //.collect(Collectors.joining(","));
        List<String> arithmetic = List.of("+", "-", "*", "/", "%", "=", "!=", "<", "<=", ">", ">="); // find rest of functions
        return arithmetic.contains(node.getFuncName()) ? StringUtils.format("%s %s %s", arguments.get(0), node.getFuncName(), arguments.get(1)) :
                StringUtils.format("%s(%s)", node.getFuncName(), String.join(",", arguments));
    }

    @Override
    public String visitCompare(Compare node, String context) {
        String left = analyze(node.getLeft(), context);
        String right = analyze(node.getRight(), context);
        return StringUtils.format("%s %s %s", left, node.getOperator(), right);
    }

    @Override
    public String visitField(Field node, String context) {
        return node.getField().toString();
    }

    @Override
    public String visitAlias(Alias node, String context) {
        String expr = node.getDelegated().accept(this, context);
        return StringUtils.format("%s", expr);
    }

    @Override
    public String visitAllFields(AllFields node, String context) {
        return "*";
    }

    @Override
    public String visitLimit(Limit node, String context) {
        String child = node.getChild().get(0).accept(this, context);
        return node.getOffset() == 0 ? StringUtils.format("%s LIMIT number", child) : StringUtils.format("%s LIMIT number, number", child);
    }
}