package ca.waterloo.dsg.graphflow.parser;

import ca.waterloo.dsg.graphflow.grammar.GraphflowBaseVisitor;
import ca.waterloo.dsg.graphflow.grammar.GraphflowParser;
import ca.waterloo.dsg.graphflow.grammar.GraphflowParser.*;
import ca.waterloo.dsg.graphflow.parser.query.OrderByConstraint;
import ca.waterloo.dsg.graphflow.parser.query.OrderByConstraint.OrderType;
import ca.waterloo.dsg.graphflow.parser.query.QueryOperation;
import ca.waterloo.dsg.graphflow.parser.query.expressions.AliasExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.ArithmeticExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.ArithmeticExpression.ArithmeticOperator;
import ca.waterloo.dsg.graphflow.parser.query.expressions.BooleanConnectorExpression.ANDExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.BooleanConnectorExpression.ORExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.ComparisonExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.FunctionInvocation;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NotExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.SimpleVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.BooleanLiteral;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.DoubleLiteral;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.IntLiteral;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.LiteralTerm;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.StringLiteral;
import ca.waterloo.dsg.graphflow.parser.query.indexquery.CreateBPTNodeIndexQuery;
import ca.waterloo.dsg.graphflow.parser.query.regularquery.PlainRegularQuery;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.AggregationFunction;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.PlainReturnOrWith.PlainReturnBody;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.PlainReturnOrWith.PlainWith;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.PlainQueryPart;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.PlainSingleQuery;
import ca.waterloo.dsg.graphflow.plan.operator.sink.AbstractUnion;
import ca.waterloo.dsg.graphflow.storage.graph.GraphCatalog;
import ca.waterloo.dsg.graphflow.util.datatype.ComparisonOperator;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * This class implements the ANTLR4 methods used to traverse the parseAntlr tree.
 */
public class ParseTreeVisitor extends GraphflowBaseVisitor<ParserMethodReturnValue> {

    private GraphCatalog catalog;

    public static final String _gFUncapitalized = "_gf";
    private static final String _gFV = "_gFV";
    private static final String _gFE = "_gFE";
    private int nextQueryNodeNameIndex = 0;
    private int nextQueryRelNameIndex = 0;

    ParseTreeVisitor(GraphCatalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public ParserMethodReturnValue visitOC_Cypher(OC_CypherContext ctx) {
        var ocQueryCtx = ctx.oC_Statement().oC_Query();
        if (null != ocQueryCtx.oC_RegularQuery()) {
            var plainRegularQuery = new PlainRegularQuery();
            plainRegularQuery.plainSingleQueries.add((PlainSingleQuery)
                visit(ocQueryCtx.oC_RegularQuery().oC_SingleQuery()));
            AbstractUnion.UnionType unionType = null;
            if (!ocQueryCtx.oC_RegularQuery().oC_Union().isEmpty()) {
                unionType = null != ocQueryCtx.oC_RegularQuery().oC_Union(0).ALL() ?
                    AbstractUnion.UnionType.UNION_ALL : AbstractUnion.UnionType.UNION;
                for (int i = 1; i < ocQueryCtx.oC_RegularQuery().oC_Union().size(); ++i) {
                    var curUnionType = null != ocQueryCtx.oC_RegularQuery().oC_Union(i).ALL() ?
                        AbstractUnion.UnionType.UNION_ALL : AbstractUnion.UnionType.UNION;
                    if (unionType != curUnionType) {
                        throw new MalformedQueryException(
                            "Invalid combination of UNION and UNION ALL");
                    }
                }
                for (int i = 0; i < ocQueryCtx.oC_RegularQuery().oC_Union().size(); ++i) {
                    plainRegularQuery.plainSingleQueries.add((PlainSingleQuery)
                        visit(ocQueryCtx.oC_RegularQuery().oC_Union(i).oC_SingleQuery()));
                }
            }
            plainRegularQuery.setUnionType(unionType);
            return plainRegularQuery;
        } else if (null != ocQueryCtx.gF_bplusTreeNodeIndexQuery()) {
            return visit(ocQueryCtx.gF_bplusTreeNodeIndexQuery());
        } else {
            throw new UnsupportedOperationException("The query is not one of the supported " +
                "queries: oC_SingleQuery, gF_adjListIndexQuery, and gF_bplusTreeIndexQuery.");
        }
    }

    @Override
    public PlainSingleQuery visitOC_SingleQuery(GraphflowParser.OC_SingleQueryContext ctx) {
        var query = new PlainSingleQuery();
        if (null != ctx.oC_SinglePartQuery()) {
            query.plainQueryParts.add((PlainQueryPart) visit(ctx.oC_SinglePartQuery()));
        } else {
            var multiPartCtx = ctx.oC_MultiPartQuery();
            for (int i = 0; i < multiPartCtx.oC_ReadingClause().size(); ++i) {
               var plainQpart = visitOC_ReadingClause(multiPartCtx.oC_ReadingClause(i));
               var ocWithCtx = multiPartCtx.oC_With(i);
               plainQpart.setPlainWith((PlainReturnBody) visit(ocWithCtx.oC_ReturnBody()));
               if (null != ocWithCtx.oC_Where()) {
                   ((PlainWith) plainQpart.getPlainReturnOrWith()).setWhereExpression(
                       (Expression) visit(ocWithCtx.oC_Where().oC_Expression().oC_OrExpression()));
               }
               query.plainQueryParts.add(plainQpart);
            }
            query.plainQueryParts.add((PlainQueryPart) visit(multiPartCtx.oC_SinglePartQuery()));
        }
        return query;
    }

    @Override
    public PlainQueryPart visitOC_SinglePartQuery(OC_SinglePartQueryContext singlePartQuery) {
        PlainQueryPart plainQpart;
        if (null != singlePartQuery.oC_ReadingClause()) {
            plainQpart = (PlainQueryPart) visit(singlePartQuery.oC_ReadingClause());
        } else {
            plainQpart = new PlainQueryPart();
        }
        plainQpart.setPlainReturn((PlainReturnBody) visit(singlePartQuery.oC_Return().oC_ReturnBody()));
        return plainQpart;
    }

    @Override
    public PlainReturnBody visitOC_ReturnBody(OC_ReturnBodyContext ctx) {
        PlainReturnBody plainReturnBody = visitOC_ReturnItems(ctx.oC_ReturnItems());
        if (null != ctx.oC_Order()) {
            ctx.oC_Order().oC_SortItem().forEach(sortItem -> {
                var expression = (Expression) visit(sortItem.oC_Expression().oC_OrExpression());
                if (null != sortItem.DESC() || null != sortItem.DESCENDING()) {
                    plainReturnBody.addOrderByConstraints(
                        new OrderByConstraint(expression, OrderType.DESCENDING));
                } else {
                    plainReturnBody.addOrderByConstraints(
                        new OrderByConstraint(expression, OrderType.ASCENDING));
                }
            });
        }
        if (null != ctx.oC_Skip()) {
            plainReturnBody.setNumTuplesToSkip(Long.parseLong(ctx.oC_Skip().oC_IntegerLiteral().getText()));
        }
        if (null != ctx.oC_Limit()) {
            plainReturnBody.setNumTuplesToLimit(Long.parseLong(ctx.oC_Limit().oC_IntegerLiteral().getText()));
        }
        return plainReturnBody;
    }

    @Override
    public PlainReturnBody visitOC_ReturnItems(OC_ReturnItemsContext ctx) {
        PlainReturnBody plainReturnBody;
        if (null != ctx.STAR()) {
            plainReturnBody = new PlainReturnBody();
            plainReturnBody.setReturnStar(true);
        } else {
            plainReturnBody = new PlainReturnBody();
            ctx.oC_ReturnItem().forEach(returnItem -> {
                var expr = (Expression) visit(returnItem.oC_Expression().oC_OrExpression());
                if (null != returnItem.AS()) {
                    expr = new AliasExpression(expr, returnItem.gF_Variable().getText());
                }
                plainReturnBody.addExpression(expr);
            });
        }
        return plainReturnBody;
    }

    private PropertyVariable constructPropertyVariable(SimpleVariable nodeOrRelVariable,
        String propertyName) {
        return new PropertyVariable(nodeOrRelVariable, propertyName);
    }

    @Override
    public CreateBPTNodeIndexQuery visitGF_bplusTreeNodeIndexQuery(
        GF_bplusTreeNodeIndexQueryContext ctx) {
        return (CreateBPTNodeIndexQuery) visit(ctx.gF_bPlusTreeNodeIndexPattern());
    }

    @Override
    public CreateBPTNodeIndexQuery visitGF_bPlusTreeNodeIndexPattern(
        GF_bPlusTreeNodeIndexPatternContext ctx) {
        CreateBPTNodeIndexQuery createBPTNodeIndexQuery = new CreateBPTNodeIndexQuery();
        createBPTNodeIndexQuery.setIndexName(ctx.gF_indexName().gF_Variable().getText());
        createBPTNodeIndexQuery.setPropertyKey(catalog.getNodePropertyKey(
            ctx.oC_PropertyOrLabelsExpression().oC_PropertyLookup().gF_Variable().getText()));
        createBPTNodeIndexQuery.setQueryOperation(QueryOperation.CREATE_BPLUS_TREE_NODE_INDEX);
        var nodePattern = ctx.oC_NodePattern();
        var matchGraphSchema = createBPTNodeIndexQuery.getInputSchemaMatchWhere().getMatchGraphSchema();
        if (null == nodePattern.oC_NodeType()) {
            throw new MalformedQueryException("Node Type should be given for variable: " +
                nodePattern.gF_Variable().getText() + ".");
        }
        matchGraphSchema.addNodeVariable(new NodeVariable(nodePattern.gF_Variable().getText(),
            catalog.getTypeKey(nodePattern.oC_NodeType().gF_Variable().getText())));
        return createBPTNodeIndexQuery;
    }

    @Override
    public PlainQueryPart visitOC_ReadingClause(OC_ReadingClauseContext oC_readingClauseCtx) {
        OC_MatchContext matchCtx = oC_readingClauseCtx.oC_Match();
        var plainQPart = new PlainQueryPart();
        for (var i = 0; i < matchCtx.oC_Pattern().oC_RelationshipPattern().size(); i++) {
            plainQPart.getRelVariables().add(
                (RelVariable) visit(matchCtx.oC_Pattern().oC_RelationshipPattern(i)));
        }
        if (null != matchCtx.oC_Where()) {
            plainQPart.setWhereExpression((Expression)
                visit(matchCtx.oC_Where().oC_Expression().oC_OrExpression()));
        }
        return plainQPart;
    }

    @Override
    public RelVariable visitOC_RelationshipPattern(OC_RelationshipPatternContext ctx) {
        var srcNodeVarName = validateAndIfNecessaryAssignNewQNodeVariable(
            ctx.oC_NodePattern(0).gF_Variable().getText());
        var srcNodeType = GraphCatalog.ANY;
        if (null != ctx.oC_NodePattern(0).oC_NodeType()) {
            srcNodeType = catalog.getTypeKey(ctx.oC_NodePattern(0).oC_NodeType().gF_Variable()
                .getText());
        }
        var srcNodeVariable = new NodeVariable(srcNodeVarName, srcNodeType);
        var dstNodeVarName = validateAndIfNecessaryAssignNewQNodeVariable(
            ctx.oC_NodePattern(1).gF_Variable().getText());
        var dstNodeType = GraphCatalog.ANY;
        if (null != ctx.oC_NodePattern(1).oC_NodeType()) {
            dstNodeType = catalog.getTypeKey(ctx.oC_NodePattern(1).oC_NodeType().gF_Variable()
                .getText());
        }
        var dstNodeVariable = new NodeVariable(dstNodeVarName, dstNodeType);
        String relVarName = null;
        if (null != ctx.oC_RelationshipDetail().gF_Variable()) {
            relVarName = ctx.oC_RelationshipDetail().gF_Variable().getText();
        }
        relVarName = validateAndIfNecessaryAssignNewQRelVariable(relVarName);
        var relLabel = catalog.getLabelKey(ctx.oC_RelationshipDetail().oC_RelationshipLabel()
            .gF_Variable().getText());
        return new RelVariable(relVarName, relLabel, srcNodeVariable, dstNodeVariable);
    }

    private String validateAndIfNecessaryAssignNewQNodeVariable(String userGivenQNodeVariable) {
        validateUserGivenVariable(userGivenQNodeVariable);
        return (null == userGivenQNodeVariable || userGivenQNodeVariable.isEmpty()) ?
            _gFV + nextQueryNodeNameIndex++ : userGivenQNodeVariable;
    }

    private String validateAndIfNecessaryAssignNewQRelVariable(String userGivenQRelVariable) {
        validateUserGivenVariable(userGivenQRelVariable);
        return (null == userGivenQRelVariable || userGivenQRelVariable.isEmpty()) ?
            _gFE + nextQueryRelNameIndex++ : userGivenQRelVariable;
    }

    private void validateUserGivenVariable(String userGivenVariable) {
        if (null != userGivenVariable && userGivenVariable.toLowerCase().startsWith(_gFUncapitalized)) {
            throw new MalformedQueryException("Variables in the query cannot start with " +
                " the substring _gF. variable: " + userGivenVariable);
        }
    }

    @Override
    public Expression visitOC_OrExpression(OC_OrExpressionContext ctx) {
        if (null != ctx.oC_AndExpression()) {
            return (Expression) visit(ctx.oC_AndExpression());
        }
        return new ORExpression((Expression) visit(ctx.oC_OrExpression(0)),
            (Expression) visit(ctx.oC_OrExpression(1)));
    }

    @Override
    public Expression visitOC_AndExpression(OC_AndExpressionContext ctx) {
        if (null != ctx.oC_NotExpression()) {
            return (Expression) visit(ctx.oC_NotExpression());
        }
        return new ANDExpression((Expression) visit(ctx.oC_AndExpression(0)),
            (Expression) visit(ctx.oC_AndExpression(1)));
    }

    @Override
    public Expression visitOC_NotExpression(OC_NotExpressionContext ctx) {
        if (null != ctx.NOT()) {
            return new NotExpression((Expression) visit(ctx.oC_ComparisonExpression()));
        }
        return (Expression) visit(ctx.oC_ComparisonExpression());
    }

    @Override
    public Expression visitOC_ComparisonExpression(OC_ComparisonExpressionContext ctx) {
        if (ctx.oC_AddOrSubtractExpression().size() == 1) {
            return (Expression) visit(ctx.oC_AddOrSubtractExpression(0));
        }
        var comparisonOperator =
            ComparisonOperator.fromString(ctx.gF_Comparison().getText().toUpperCase());
        var leftExpression = (Expression) visit(ctx.oC_AddOrSubtractExpression().get(0));
        var rightExpression = (Expression) visit(ctx.oC_AddOrSubtractExpression().get(1));
        var comparisonExpr = new ComparisonExpression(comparisonOperator, leftExpression,
            rightExpression);
        if (comparisonExpr.getLeftExpression() instanceof LiteralTerm &&
            comparisonExpr.getRightExpression() instanceof LiteralTerm) {
            throw new MalformedQueryException("Both the right and left expressions of a " +
                "comparison expression cannot be literals. leftExpression: "
                + comparisonExpr.getLeftExpression().getVariableName() + " rightExpression: "
                + comparisonExpr.getRightExpression().getVariableName());
        }
        return comparisonExpr;
    }

    @Override
    public Expression visitOC_AddOrSubtractExpression(OC_AddOrSubtractExpressionContext ctx) {
        if (null != ctx.oC_MultiplyDivideModuloExpression()) {
            return (Expression) visit(ctx.oC_MultiplyDivideModuloExpression());
        }
        var arithmeticOp = null != ctx.PLUS() ? ArithmeticOperator.ADD : ArithmeticOperator.SUBTRACT;
        return parseBinaryArithmeticExpression(arithmeticOp,
                ctx.oC_AddOrSubtractExpression(0), ctx.oC_AddOrSubtractExpression(1));
    }

    @Override
    public Expression visitOC_MultiplyDivideModuloExpression(OC_MultiplyDivideModuloExpressionContext ctx) {
        if (null != ctx.oC_PowerOfExpression()) {
            return (Expression) visit(ctx.oC_PowerOfExpression());
        }
        var arithmeticOp = null != ctx.STAR() ? ArithmeticOperator.MULTIPLY :
                (null != ctx.FORWARD_SLASH() ? ArithmeticOperator.DIVIDE : ArithmeticOperator.MODULO);
        return parseBinaryArithmeticExpression(arithmeticOp, ctx.oC_MultiplyDivideModuloExpression(0),
            ctx.oC_MultiplyDivideModuloExpression(1));
    }

    @Override
    public Expression visitOC_PowerOfExpression(OC_PowerOfExpressionContext ctx) {
        if (ctx.gF_UnaryNegationExpression().size() > 1) {
            return parseBinaryArithmeticExpression(ArithmeticOperator.POWER,
                ctx.gF_UnaryNegationExpression(0), ctx.gF_UnaryNegationExpression(1));
        }
        return (Expression) visit(ctx.gF_UnaryNegationExpression(0));
    }

    private Expression parseBinaryArithmeticExpression(ArithmeticOperator arithmeticOp,
        ParseTree leftTree, ParseTree rightTree) {
        var leftExpression = (Expression) visit(leftTree);
        var rightExpression = (Expression) visit(rightTree);
        return new ArithmeticExpression(arithmeticOp, leftExpression,
            rightExpression);
    }

    @Override
    public Expression visitGF_UnaryNegationExpression(GF_UnaryNegationExpressionContext ctx) {
        if (null != ctx.oC_Dash()) {
            var leftExpression = new IntLiteral(-1);
            var rightExpression = (Expression) visit(ctx.oC_PropertyOrLabelsExpression());
            return new ArithmeticExpression(ArithmeticOperator.MULTIPLY,
                leftExpression, rightExpression);
        }
        return (Expression) visit(ctx.oC_PropertyOrLabelsExpression());
    }

    @Override
    public Expression visitOC_PropertyOrLabelsExpression(OC_PropertyOrLabelsExpressionContext ctx) {
        var atom = (Expression) visit(ctx.oC_Atom());
        if (null != ctx.oC_PropertyLookup()) {
            if (!(atom instanceof SimpleVariable)) {
                throw new MalformedQueryException("");
            }
            return constructPropertyVariable((SimpleVariable) atom,
                ctx.oC_PropertyLookup().gF_Variable().getText());
        }
        return atom;
    }

    @Override
    public Expression visitOC_Atom(OC_AtomContext ctx) {
        if (null != ctx.oC_Literal()) {
            return (Expression) visit(ctx.oC_Literal());
        } else if (null != ctx.gF_Variable()) {
            return (Expression) visit(ctx.gF_Variable());
        } else if (null != ctx.COUNT()) {
            return FunctionInvocation.newCountStar();
        } else if (null != ctx.oC_FunctionInvocation()) {
            return (Expression) visit(ctx.oC_FunctionInvocation());
        } else if (null != ctx.oC_ParenthesizedExpression()) {
            return (Expression) visit(ctx.oC_ParenthesizedExpression().oC_Expression());
        }
        throw new MalformedQueryException("This should never happen!");
    }

    @Override
    public FunctionInvocation visitOC_FunctionInvocation(OC_FunctionInvocationContext ctx) {
        return new FunctionInvocation(AggregationFunction.getFromString(ctx.oC_FunctionName().getText()),
            (Expression) visit(ctx.oC_Expression()));
    }

    @Override
    public SimpleVariable visitGF_Variable(GF_VariableContext ctx) {
        return new SimpleVariable(ctx.getText());
    }

    @Override
    public Expression visitOC_Literal(OC_LiteralContext ctx) {
        if (null != ctx.StringLiteral()) {
            var quotedString = ctx.getText();
            return new StringLiteral(quotedString.substring(1, quotedString.length() - 1));
        } else if (null != ctx.oC_BooleanLiteral()) {
            return new BooleanLiteral(Boolean.parseBoolean(ctx.getText()));
        } else {
            if (null != ctx.oC_NumberLiteral().oC_IntegerLiteral()) {
                var numLiteralCtx = ctx.oC_NumberLiteral();
                var literal = numLiteralCtx.getText();
                return new IntLiteral(Integer.parseInt(literal));
            } else {
                var numLiteralCtx = ctx.oC_NumberLiteral();
                var literal = numLiteralCtx.oC_DoubleLiteral().getText();
                return new DoubleLiteral(Double.parseDouble(literal));
            }
        }
    }
}
