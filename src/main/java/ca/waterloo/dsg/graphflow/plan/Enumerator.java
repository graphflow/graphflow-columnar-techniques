package ca.waterloo.dsg.graphflow.plan;

import ca.waterloo.dsg.graphflow.datachunk.PhysicalSchema;
import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.parser.query.regularquery.RegularQuery;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.ReturnOrWith.ReturnBody.ReturnBodyType;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.InputSchemaMatchWhere;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.QueryPart;
import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.extend.ExtendAdjLists;
import ca.waterloo.dsg.graphflow.plan.operator.extend.ExtendColumn;
import ca.waterloo.dsg.graphflow.plan.operator.filter.Filter;
import ca.waterloo.dsg.graphflow.plan.operator.flatten.Flatten;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.NodePropertyReader;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.RelPropertyReader;
import ca.waterloo.dsg.graphflow.plan.operator.scan.Scan;
import ca.waterloo.dsg.graphflow.plan.operator.sink.SinkCopy;
import ca.waterloo.dsg.graphflow.plan.operator.sink.SinkCountChunks;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.util.collection.SetUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static ca.waterloo.dsg.graphflow.storage.GraphCatalog.ANY;

public class Enumerator {

    private final Graph graph;
    private final QueryPart query;

    private final PhysicalSchema[] physicalSchemaPerStage;
    private final InputSchemaMatchWhere[] schemaPerExtension;

    private final Set<String> filteredVariables = new HashSet<>();
    private final Set<Integer> filteredDataChunkPos = new HashSet<>();
    private final List<Expression> predicateClausesApplied = new ArrayList<>();

    private final Set<NodeVariable> nodes;

    public Enumerator(RegularQuery query, Graph graph) {
        this.graph = graph;
        this.query = query.singleQueries.get(0).queryParts.get(0);
        var whereExpr = this.query.getInputSchemaMatchWhere().getWhereExpression();
        if (whereExpr != null) {
            filteredVariables.addAll(whereExpr.getDependentVariableNames());
        }
        nodes = this.query.getInputSchemaMatchWhere().getMatchGraphSchema().getNodeVariables();
        physicalSchemaPerStage = new PhysicalSchema[nodes.size()];
        schemaPerExtension = new InputSchemaMatchWhere[nodes.size()];
    }

    public List<Operator> generatePlans() {
        var nodeNames = new String[nodes.size()];
        var idx = 0;
        for (var node : nodes) {
            nodeNames[idx++] = node.getVariableName();
        }
        return SetUtils.
            generatePermutations(nodeNames).
            stream().
            filter(QVO -> isConnected(QVO)).
            map(QVO -> generatePlan(QVO)).
            collect(Collectors.toList());
    }

    public Operator generatePlan(String[] QVO) {
        reset();
        var lastOperator = appendScanAndFilterIfAny(QVO);
        for (var extensionIdx = 1; extensionIdx < QVO.length; extensionIdx++) {
            lastOperator = appendExtend(QVO, extensionIdx, lastOperator);
        }
        return appendSink(lastOperator, QVO);
    }

    private Operator appendScanAndFilterIfAny(String[] QVO) {
        var nodeName = QVO[0];
        var nodeVar = query.getMatchGraphSchema().getNodeVariable(nodeName);
        var scan = new Scan(nodeVar, shouldResetSelector(nodeVar) ||
            resetDueToColumnExtend(nodeName, 0 /*extensionIdx*/, QVO));
        physicalSchemaPerStage[0] = new PhysicalSchema();
        physicalSchemaPerStage[0].addVariable(nodeName, 0 /*dataChunkPos*/, 0 /*vectorPos*/);
        physicalSchemaPerStage[0].dataChunkPosToIsFlatMap.put(0 /*dataChunkPos*/, false);
        var nodeVars = new HashSet<NodeVariable>();
        nodeVars.add(nodeVar);
        schemaPerExtension[0] = query.getInputSchemaMatchWhere().getProjection(nodeVars);
        Operator lastOperator = scan;
        var whereExpr = schemaPerExtension[0].getWhereExpression();
        if (null != whereExpr) {
            var filterExprs = whereExpr.splitPredicateClauses();
            predicateClausesApplied.addAll(filterExprs);
            for (var filterExpr : filterExprs) {
                var isFiltered = filteredDataChunkPos.contains(0 /* dataChunkPos */);
                var propertyVars = filterExpr.getDependentPropertyVars();
                for (var propertyVar : propertyVars) {
                    if (!physicalSchemaPerStage[0].hasVariable(propertyVar.getPropertyName())) {
                        lastOperator = NodePropertyReader.make(propertyVar, false /*isFlat*/,
                            isFiltered, graph.getNodePropertyStore(), lastOperator);
                        physicalSchemaPerStage[0].addVariable(propertyVar.getPropertyName(),
                            0 /*dataChunkPos*/, physicalSchemaPerStage[0].getNumVectors(0));
                    }
                }
                lastOperator = new Filter(filterExpr, false /*isFlat*/, lastOperator);
                filteredDataChunkPos.add(0 /*dataChunkPos*/);
            }
        }
        return lastOperator;
    }

    public Operator appendExtend(String[] QVO, int extsIdx, Operator lastOperator) {
        var toVar = QVO[extsIdx];
        var prevSchema = physicalSchemaPerStage[extsIdx - 1];
        var currSchema = physicalSchemaPerStage[extsIdx] = prevSchema.clone();
        for (var i = 0; i < extsIdx; i++) {
            var fromVar = QVO[i];
            var relVar = query.getMatchGraphSchema().getRelVariable(fromVar, toVar);
            if (relVar != null) {
                var ALD = makeALD(relVar, fromVar);
                var catalog = graph.getGraphCatalog();
                var boundVarName = ALD.getBoundNodeVariable().getVariableName();
                var toVarName = ALD.getToNodeVariable().getVariableName();
                var relName = ALD.getRelVariable().getVariableName();
                var label = relVar.getLabel();
                var direction = ALD.getDirection();
                var numNbrTypes = catalog.getNumNbrTypes(label, direction);
                var typeFilter = numNbrTypes > 1 ? ALD.getToNodeVariable().getType() : ANY;
                if (catalog.labelDirectionHasMultiplicityOne(label, direction)) {
                    lastOperator = new ExtendColumn(ALD, typeFilter, lastOperator);
                    var chunkPos = prevSchema.getDataChunkPos(boundVarName);
                    currSchema.addVariable(toVar, chunkPos,
                        prevSchema.getNumVectors(chunkPos) /* new vector pos */);
                    filteredDataChunkPos.add(chunkPos);
                } else {
                    var prevChunkPos = prevSchema.getDataChunkPos(boundVarName);
                    if (!prevSchema.dataChunkPosToIsFlatMap.get(prevChunkPos)) {
                        lastOperator = new Flatten(boundVarName, lastOperator);
                        currSchema.dataChunkPosToIsFlatMap.put(prevChunkPos, true /*isFlat*/);
                    }
                    lastOperator = new ExtendAdjLists(ALD, typeFilter, shouldResetSelector(
                        toVarName, relName) || resetDueToColumnExtend(toVarName, extsIdx, QVO),
                        lastOperator);
                    var newDataChunkPos = prevSchema.dataChunkPosToIsFlatMap.size();
                    currSchema.addVariable(relName, newDataChunkPos, 0 /* new vector pos */);
                    currSchema.addVariable(toVarName, newDataChunkPos, 1 /* new vector pos */);
                    currSchema.dataChunkPosToIsFlatMap.put(newDataChunkPos, false /*isFlat*/);
                }
                break;
            }
        }

        var nodeVars = new HashSet<NodeVariable>();
        for (var i = 0; i <= extsIdx; i++) {
            nodeVars.add(query.getMatchGraphSchema().getNodeVariable(QVO[i]));
        }
        schemaPerExtension[extsIdx] = query.getInputSchemaMatchWhere().getProjection(nodeVars);
        var whereExpr = schemaPerExtension[extsIdx].getWhereExpression();
        if (whereExpr != null) {
            var predicateClausesToApply = SetUtils.subtract(whereExpr.splitPredicateClauses(),
                predicateClausesApplied);
            predicateClausesApplied.addAll(predicateClausesToApply);
            for (var predicateClause : predicateClausesToApply) {
                var propertyVars = predicateClause.getDependentPropertyVars();
                for (var propertyVar : propertyVars) {
                    var varName = propertyVar.getVariableName();
                    if (!currSchema.hasVariable(varName)) {
                        var isNodeProperty = propertyVar.getNodeOrRelVariable() instanceof NodeVariable;
                        var nodeOrRelName = propertyVar.getNodeOrRelVariable().getVariableName();
                        var dataChunkPos = currSchema.getDataChunkPos(nodeOrRelName);
                        lastOperator = isNodeProperty ? NodePropertyReader.make(propertyVar,
                            currSchema.dataChunkPosToIsFlatMap.get(dataChunkPos) /*isFlat*/,
                            true, //filteredDataChunkPos.contains(dataChunkPos),
                            graph.getNodePropertyStore(), lastOperator) :
                            RelPropertyReader.make(propertyVar,
                                currSchema.dataChunkPosToIsFlatMap.get(dataChunkPos) /*isFlat*/,
                                true, //filteredDataChunkPos.contains(dataChunkPos) /*isFiltered*/,
                                graph.getGraphCatalog(), lastOperator);
                        currSchema.addVariable(varName, dataChunkPos, currSchema.getNumVectors(
                            dataChunkPos));
                    }
                }
                var numUnflatVars = 0;
                for (var propertyVar : propertyVars) {
                    numUnflatVars += currSchema.dataChunkPosToIsFlatMap.get(
                        currSchema.getDataChunkPos(propertyVar.getVariableName())) ? 0 : 1;
                }
                var it = propertyVars.iterator();
                while (numUnflatVars > 1) {
                    var name = it.next().getVariableName();
                    lastOperator = new Flatten(name, lastOperator);
                    physicalSchemaPerStage[extsIdx].dataChunkPosToIsFlatMap.put(
                        physicalSchemaPerStage[extsIdx].getDataChunkPos(name), true /*isFlat*/);
                    numUnflatVars--;
                }

                int unflatDataChunkPos = -1;
                for (var propertyVar : propertyVars) {
                    var name = propertyVar.getNodeOrRelVariable().getVariableName();
                    var chunkPos = physicalSchemaPerStage[extsIdx].getDataChunkPos(name);
                    if (!physicalSchemaPerStage[extsIdx].dataChunkPosToIsFlatMap.get(chunkPos)) {
                        unflatDataChunkPos = chunkPos;
                        break;
                    }
                }
                if (unflatDataChunkPos != -1) {
                    filteredDataChunkPos.add(unflatDataChunkPos);
                }
                lastOperator = new Filter(predicateClause, unflatDataChunkPos == -1 /*isFlat*/,
                    lastOperator);
            }
        }
        return lastOperator;
    }

    private Operator appendSink(Operator lastOperator, String[] QVO) {
        var schema = physicalSchemaPerStage[physicalSchemaPerStage.length - 1];
        var returnBody = query.getReturnBody();
        if (returnBody.getReturnBodyType() == ReturnBodyType.GROUP_BY_AND_AGGREGATE) {
            lastOperator = new SinkCountChunks(QVO, lastOperator);
        } else {
            var propertyVars = returnBody.getProjectionExpressions().getProjectionExpressions();
            for (var propertyVar : propertyVars) {
                if (!schema.hasVariable(propertyVar.getVariableName())) {
                    var varName =
                        ((PropertyVariable) propertyVar).getNodeOrRelVariable().getVariableName();
                    var isNodeProperty = ((PropertyVariable) propertyVar).getNodeOrRelVariable() instanceof NodeVariable;
                    var dataChunkPos = schema.getDataChunkPos(varName);
                    lastOperator = isNodeProperty ?
                        NodePropertyReader.make(((PropertyVariable) propertyVar),
                        schema.dataChunkPosToIsFlatMap.get(dataChunkPos) /*isFlat*/,
                        true, //filteredDataChunkPos.contains(dataChunkPos),
                        graph.getNodePropertyStore(), lastOperator) :
                        RelPropertyReader.make((PropertyVariable) propertyVar,
                            schema.dataChunkPosToIsFlatMap.get(dataChunkPos) /*isFlat*/,
                            true,
                            //filteredDataChunkPos.contains(dataChunkPos) /*isFiltered*/,
                            graph.getGraphCatalog(), lastOperator);
                    ;
                    schema.addVariable(propertyVar.getVariableName(), dataChunkPos, schema.
                        getNumVectors(dataChunkPos));
                }
            }

            for (var dataChunkPos : schema.dataChunkPosToIsFlatMap.keySet()) {
                if (!schema.dataChunkPosToIsFlatMap.get(dataChunkPos)) {
                    lastOperator = new Flatten(schema.getAnyVariable(dataChunkPos), lastOperator);
                }
            }

            var catalog = graph.getGraphCatalog();
            var intList = new ArrayList<String>();
            var strList = new ArrayList<String>();
            var nodeList = new ArrayList<String>();
            for (var propertyVar : propertyVars) {
                if (propertyVar instanceof PropertyVariable) {
                    var propertyName = ((PropertyVariable) propertyVar).getPropertyName();
                    var nodeOrRelVariable = ((PropertyVariable) propertyVar).getNodeOrRelVariable();
                    var dataType = nodeOrRelVariable instanceof NodeVariable ?
                        catalog.getNodePropertyDataType(catalog.getNodePropertyKey(propertyName)) :
                        catalog.getRelPropertyDataType(catalog.getRelPropertyKey(propertyName));
                    switch (dataType) {
                        case INT:
                            intList.add(propertyVar.getVariableName());
                            break;
                        case STRING:
                            strList.add(propertyVar.getVariableName());
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported data type in SinkCopy.");
                    }
                } else {
                    nodeList.add(propertyVar.getVariableName());
                }
            }
            lastOperator = new SinkCopy(getAsArr(intList), getAsArr(strList), getAsArr(nodeList),
                QVO, lastOperator);
        }
        return lastOperator;
    }

    private boolean shouldResetSelector(NodeVariable variable) {
        return filteredVariables.contains(variable.getVariableName());
    }

    private boolean shouldResetSelector(String toVarName, String relVarName) {
        return filteredVariables.contains(toVarName) || filteredVariables.contains(relVarName);
    }

    private void reset() {
        filteredDataChunkPos.clear();
        predicateClausesApplied.clear();
    }

    public boolean isConnected(String[] QVO) {
        for (var i = QVO.length - 1; i > 0; i--) {
            var fromVar = QVO[i];
            var isConnected = false;
            for (var j = 0; j < i; j++) {
                var toVar = QVO[j];
                if (query.getMatchGraphSchema().getRelVariable(fromVar, toVar) != null) {
                    isConnected = true;
                    break;
                }
            }
            if (!isConnected) {
                return false;
            }
        }
        return true;
    }

    public static AdjListDescriptor makeALD(RelVariable relVar, String variable) {
        var direction = variable.equals(relVar.getSrcNode().getVariableName()) ?
            Direction.FORWARD : Direction.BACKWARD;
        var boundVar = direction == Direction.FORWARD ? relVar.getSrcNode() : relVar.getDstNode();
        var nbrVar = direction == Direction.FORWARD ? relVar.getDstNode() : relVar.getSrcNode();
        return new AdjListDescriptor(relVar, boundVar, nbrVar, direction);
    }

    private boolean resetDueToColumnExtend(String toVar, int extensionIdx, String[] QVO) {
        for (var i = 0; i < extensionIdx; i++) {
            var relVar = query.getInputSchemaMatchWhere().getMatchGraphSchema().getRelVariable(
                toVar, QVO[i]);
            if (relVar != null) {
                var direction = toVar.equals(relVar.getDstNode().getVariableName()) ?
                    Direction.FORWARD : Direction.BACKWARD;
                var label = relVar.getLabel();
                if (graph.getGraphCatalog().labelDirectionHasMultiplicityOne(label, direction)) {
                    return false;
                }
            }
        }
        for (var i = extensionIdx + 1; i < QVO.length; i++) {
            var relVar = query.getInputSchemaMatchWhere().getMatchGraphSchema().getRelVariable(
                toVar, QVO[i]);
            if (relVar != null) {
                var direction = toVar.equals(relVar.getSrcNode().getVariableName()) ?
                    Direction.FORWARD : Direction.BACKWARD;
                var label = relVar.getLabel();
                if (graph.getGraphCatalog().labelDirectionHasMultiplicityOne(label, direction)) {
                    return true;
                }
            }
        }
        return false;
    }

    private String[] getAsArr(List<String> list) {
        var arr = new String[list.size()];
        return list.toArray(arr);
    }
}
