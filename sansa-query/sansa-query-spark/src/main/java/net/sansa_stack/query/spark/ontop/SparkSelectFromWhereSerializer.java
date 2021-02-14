package net.sansa_stack.query.spark.ontop;

import com.google.inject.Inject;
import it.unibz.inf.ontop.com.google.common.collect.ImmutableList;
import it.unibz.inf.ontop.com.google.common.collect.ImmutableMap;
import it.unibz.inf.ontop.com.google.common.collect.ImmutableSortedSet;
import it.unibz.inf.ontop.dbschema.DBParameters;
import it.unibz.inf.ontop.dbschema.QualifiedAttributeID;
import it.unibz.inf.ontop.dbschema.QuotedID;
import it.unibz.inf.ontop.dbschema.RelationID;
import it.unibz.inf.ontop.generation.algebra.*;
import it.unibz.inf.ontop.generation.serializer.SQLSerializationException;
import it.unibz.inf.ontop.generation.serializer.impl.DefaultSelectFromWhereSerializer;
import it.unibz.inf.ontop.generation.serializer.impl.SQLTermSerializer;
import it.unibz.inf.ontop.model.term.*;
import it.unibz.inf.ontop.model.term.functionsymbol.db.DBFunctionSymbol;
import it.unibz.inf.ontop.model.type.DBTermType;
import it.unibz.inf.ontop.substitution.ImmutableSubstitution;
import it.unibz.inf.ontop.utils.ImmutableCollectors;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Lorenz Buehmann
 */
public class SparkSelectFromWhereSerializer extends DefaultSelectFromWhereSerializer {

    @Inject
    private SparkSelectFromWhereSerializer(TermFactory termFactory) {
        super(new SparkSQLTermSerializer(termFactory));
    }

//    @Inject
//    protected SparkSelectFromWhereSerializer(SQLTermSerializer sqlTermSerializer) {
//        super(sqlTermSerializer);
//    }

    public static boolean offsetEnabled = true;
    public static boolean orderByWorkaround = false;

    @Override
    public QuerySerialization serialize(SelectFromWhereWithModifiers selectFromWhere, DBParameters dbParameters) {
        return selectFromWhere.acceptVisitor(
                new DefaultRelationVisitingSerializer(dbParameters.getQuotedIDFactory()) {

                    final Deque<Boolean> handleOffsetStack = new ArrayDeque<>();
                    String rowNumCol = "row_num";
                    private boolean needOffsetProcessed() {
                        return Optional.ofNullable(handleOffsetStack.peek()).orElse(false);
                    }

                    final Deque<Boolean> distinctStack = new ArrayDeque<>();
                    final Deque<ImmutableSortedSet<Variable>> projectionVariableStack = new ArrayDeque<>();
                    final Deque<ImmutableMap<Variable, QuotedID>> variableAliasesStack = new ArrayDeque<>();

                    @Override
                    public QuerySerialization visit(SelectFromWhereWithModifiers selectFromWhere) {
                        // for offset handling
                        long offset = selectFromWhere.getOffset().orElse(0L);
                        handleOffsetStack.push(offsetEnabled && offset > 0);

                        // for "distinct - order by" workaround
                        distinctStack.push(selectFromWhere.isDistinct());
                        projectionVariableStack.push(selectFromWhere.getProjectedVariables());

                        QuerySerialization querySerialization = super.visit(selectFromWhere);

                        if (needOffsetProcessed()) {
                            String query = querySerialization.getString();
                            String offsetExpression = String.format("%s > %d", rowNumCol, offset);
                            long limit = selectFromWhere.getLimit().orElse(0L);
                            if (limit > 0) {
                                offsetExpression += String.format(" AND %s < %d", rowNumCol, offset+limit);
                            }
                            String newQuery = String.format("WITH result AS (%s) SELECT * FROM result WHERE %s", query, offsetExpression);
                            querySerialization = new DefaultSelectFromWhereSerializer.QuerySerializationImpl(newQuery, querySerialization.getColumnIDs());
                        }

                        handleOffsetStack.pop();

                        projectionVariableStack.pop();
                        distinctStack.pop();
                        variableAliasesStack.pop();
                        return querySerialization;
                    }

                    @Override
                    protected String serializeProjection(ImmutableSortedSet<Variable> projectedVariables, ImmutableMap<Variable, QuotedID> variableAliases, ImmutableSubstitution<? extends ImmutableTerm> substitution, ImmutableMap<Variable, QualifiedAttributeID> columnIDs) {
                        variableAliasesStack.push(variableAliases);
                        String projection = super.serializeProjection(projectedVariables, variableAliases, substitution, columnIDs);
                        if (needOffsetProcessed()) {
                            String sortExprStr = projectedVariables.stream().map((v) ->
                                    SparkSelectFromWhereSerializer.this.sqlTermSerializer.serialize(Optional.ofNullable(substitution.get(v)).orElse(v), columnIDs))
                                    .collect(Collectors.joining(", "));
                            projection += String.format(", row_number() over (order by %s) as %s", sortExprStr, rowNumCol);
                        }
                        return projection;
                    }

                    @Override
                    protected String serializeLimit(long limit) {
                        if (limit < 0) {
                            return "";
                        } else {
                            return String.format("LIMIT %d", limit);
                        }
                    }

                    @Override
                    protected String serializeLimitOffset(long limit, long offset) {
                        if (limit == 0) {
                            return "LIMIT 0";
                        }

                        if (limit < 0) {
                            if (offset <= 0) {
                                return "";
                            } else {
                                if (!offsetEnabled) {
                                    throw new SQLSerializationException("OFFSET is not supported in Spark");
                                } else {
                                    return "";
                                }
                            }
                        } else {
                            if (offset < 0) {
                                // If the offset is not specified
                                return String.format("LIMIT %d", limit);
                            } else {
                                if (!offsetEnabled) {
                                    throw new SQLSerializationException("OFFSET is not supported in Spark");
                                } else {
                                    return "";
                                }
                            }
                        }
                    }

                    @Override
                    protected String serializeOffset(long offset) {
                        if (offset < 0) {
                            // If the offset is not specified
                            return "";
                        } else {
                            if (!offsetEnabled) {
                                throw new SQLSerializationException("OFFSET is not supported in Spark");
                            } else {
                                return "";
                            }
                        }
                    }

                    @Override
                    protected String serializeOrderBy(ImmutableList<SQLOrderComparator> sortConditions, ImmutableMap<Variable, QualifiedAttributeID> columnIDs) {
                        if (orderByWorkaround && Optional.ofNullable(distinctStack.peek()).orElse(false)) {
                            if (sortConditions.isEmpty()) {
                                return "";
                            } else {
                                ImmutableSortedSet<Variable> projectedVars = projectionVariableStack.peek();
                                ImmutableMap<Variable, QuotedID> aliases = variableAliasesStack.peek();

                                String conditionString = sortConditions.stream().map((c) -> {

                                    String s = (c.getTerm() instanceof Variable)
                                            ? (projectedVars.contains(c) ? aliases.get(c).getSQLRendering() : SparkSelectFromWhereSerializer.this.sqlTermSerializer.serialize(c.getTerm(), columnIDs))
                                            : SparkSelectFromWhereSerializer.this.sqlTermSerializer.serialize(c.getTerm(), columnIDs);

                                    return s + (c.isAscending() ? " NULLS FIRST" : " DESC NULLS LAST");
                                }).collect(Collectors.joining(", "));

//                                String conditionString = sortConditions.stream().map((c) ->
//                                    SparkSelectFromWhereSerializer.this.sqlTermSerializer.serialize(c.getTerm(), columnIDs) + (c.isAscending() ? " NULLS FIRST" : " DESC NULLS LAST")
//                                ).collect(Collectors.joining(", "));
                                return String.format("ORDER BY %s\n", conditionString);
                            }
                        }
                        return super.serializeOrderBy(sortConditions, columnIDs);
                    }

                    @Override
                    protected QuerySerialization visit(BinaryJoinExpression binaryJoinExpression, String operatorString) {
                        QuerySerialization left = getSQLSerializationForChild(binaryJoinExpression.getLeft());
                        QuerySerialization right = getSQLSerializationForChild(binaryJoinExpression.getRight());

                        ImmutableMap<Variable, QualifiedAttributeID> columnIDs = ImmutableList.of(left,right).stream()
                                .flatMap(m -> m.getColumnIDs().entrySet().stream())
                                .collect(ImmutableCollectors.toMap());

                        String onString = binaryJoinExpression.getFilterCondition()
                                .map(e -> sqlTermSerializer.serialize(e, columnIDs))
                                .map(s -> String.format("ON %s ", s))
                                .orElse("ON 1 = 1 ");

                        // Spark needs brackets as it can't handle chained ON statements
                        // A join B join C on () on ()
                        String rightStr = right.getString();
                        if (!(binaryJoinExpression.getRight() instanceof SQLTable)) {
                            rightStr = "(" + rightStr + ")";
                        }
                        String sql = String.format("%s\n %s \n%s %s", left.getString(), operatorString, rightStr, onString);
                        return new QuerySerializationImpl(sql, columnIDs);
                    }

                    //this function is required in case at least one of the children is
                    // SelectFromWhereWithModifiers expression
                    private QuerySerialization getSQLSerializationForChild(SQLExpression expression) {
                        if (expression instanceof SelectFromWhereWithModifiers) {
                            QuerySerialization serialization = expression.acceptVisitor(this);
                            RelationID alias = generateFreshViewAlias();
                            String sql = String.format("(%s) %s", serialization.getString(), alias.getSQLRendering());
                            return new QuerySerializationImpl(sql,
                                    replaceRelationAlias(alias, serialization.getColumnIDs()));
                        }
                        return expression.acceptVisitor(this);
                    }

                    private ImmutableMap<Variable, QualifiedAttributeID> replaceRelationAlias(RelationID alias, ImmutableMap<Variable, QualifiedAttributeID> columnIDs) {
                        return columnIDs.entrySet().stream()
                                .collect(ImmutableCollectors.toMap(
                                        Map.Entry::getKey,
                                        e -> new QualifiedAttributeID(alias, e.getValue().getAttribute())));
                    }
                }
        );
    }

    protected static class SparkSQLTermSerializer implements SQLTermSerializer {

        private final TermFactory termFactory;

        protected SparkSQLTermSerializer(TermFactory termFactory) {
            this.termFactory = termFactory;
        }

        @Override
        public String serialize(ImmutableTerm term, ImmutableMap<Variable, QualifiedAttributeID> columnIDs)
                throws SQLSerializationException {
            if (term instanceof Constant) {
                return serializeConstant((Constant)term);
            }
            else if (term instanceof Variable) {
                return Optional.ofNullable(columnIDs.get(term))
                        .map(QualifiedAttributeID::getSQLRendering)
                        .orElseThrow(() -> new SQLSerializationException(String.format(
                                "The variable %s does not appear in the columnIDs", term)));
            }
            /*
             * ImmutableFunctionalTerm with a DBFunctionSymbol
             */
            else {
                return Optional.of(term)
                        .filter(t -> t instanceof ImmutableFunctionalTerm)
                        .map(t -> (ImmutableFunctionalTerm) t)
                        .filter(t -> t.getFunctionSymbol() instanceof DBFunctionSymbol)
                        .map(t -> ((DBFunctionSymbol) t.getFunctionSymbol()).getNativeDBString(
                                t.getTerms(),
                                t2 -> serialize(t2, columnIDs),
                                termFactory))
                        .orElseThrow(() -> new SQLSerializationException("Only DBFunctionSymbols must be provided " +
                                "to a SQLTermSerializer"));
            }
        }

        private String serializeConstant(Constant constant) {
            if (constant.isNull())
                return constant.getValue();
            if (!(constant instanceof DBConstant)) {
                throw new SQLSerializationException(
                        "Only DBConstants or NULLs are expected in sub-tree to be translated into SQL");
            }
            return serializeDBConstant((DBConstant) constant);
        }

        protected String serializeDBConstant(DBConstant constant) {
            DBTermType dbType = constant.getType();

            switch (dbType.getCategory()) {
                case DECIMAL:
                case FLOAT_DOUBLE:
                    // TODO: handle the special case of not-a-number!
                    return castFloatingConstant(constant.getValue(), dbType);
                case INTEGER:
                case BOOLEAN:
                    return constant.getValue();
                default:
                    return serializeStringConstant(constant.getValue());
            }
        }

        protected String castFloatingConstant(String value, DBTermType dbType) {
            return String.format("CAST(%s AS %s)", value, dbType.getCastName());
        }

        protected String serializeStringConstant(String constant) {
            // duplicates single quotes, and adds outermost quotes
            return "'" + constant.replace("'", "''") + "'";
        }
    }
}
