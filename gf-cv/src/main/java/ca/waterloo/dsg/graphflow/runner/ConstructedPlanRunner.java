package ca.waterloo.dsg.graphflow.runner;

import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.plan.RegularQueryPlan;
import ca.waterloo.dsg.graphflow.plan.SingleQueryPlan;
import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.extend.FlatExtendDefaultAdjList.FlatExtendDefaultAdjListSingleType;
import ca.waterloo.dsg.graphflow.plan.operator.scan.ScanNode;
import ca.waterloo.dsg.graphflow.plan.operator.sink.Sink;
import ca.waterloo.dsg.graphflow.storage.graph.Graph.Direction;

import java.io.IOException;
import java.util.ArrayList;

// {forum=1, tagclass=7, post=5, person=3, organisation=2, comment=0, place=4, tag=6}

// {hasCreator=1, studyAt=13, hasTag=5, workAt=14, hasMember=3, isPartOf=8, hasModerator=4,
// hasInterest=2, isLocatedIn=7, containerOf=0, isSubclassOf=9, replyOf=12, hasType=6, knows=10,
// likes=11}

public class ConstructedPlanRunner extends AbstractPlanRunner {

    public static void main(String[] args) throws InterruptedException, IOException,
        ClassNotFoundException {
        var fName = "/home/p43gupta/extended/datasets/ldbc/0.1/ser-gfcore-old/";
        var plan = extend1();
        var stats = new ArrayList<Double>();
        loadDataset(fName, stats);
        var qestats = new QueryExecutionStat();
        executeASingleQueryPlan(qestats, plan, 0, 1);
        System.out.println(qestats.toString());
    }

    private static RegularQueryPlan wrapOperators(Operator lOp) {
        var sqp = new SingleQueryPlan(lOp);
        var sqps = new ArrayList<SingleQueryPlan>();
        sqps.add(sqp);
        var qp = new RegularQueryPlan();
        qp.setSingleQueryPlans(sqps);
        qp.appendSink(new Sink(lOp.getOutSchema()));
        return qp;
    }

    private static RegularQueryPlan extend1() {
        var p1 = new NodeVariable("p1", 3 /*person*/);
        var p2 = new NodeVariable("p2", 3 /*person*/);
        var k1 = new RelVariable("k1", 10 /*knows*/, p1, p2);
        var ald_k1 = new AdjListDescriptor(k1, p1, p2, Direction.FORWARD);

        var scan = new ScanNode(p1);
        var extend = new FlatExtendDefaultAdjListSingleType(ald_k1, scan.getOutSchema());
        scan.setNext(extend);
        extend.setPrev(scan);
        return wrapOperators(extend);
    }

    private static RegularQueryPlan extend2() {
        var p1 = new NodeVariable("p1", 3 /*person*/);
        var p2 = new NodeVariable("p2", 3 /*person*/);
        var p3 = new NodeVariable("p3", 3 /*person*/);
        var k1 = new RelVariable("k1", 10 /*knows*/, p1, p2);
        var ald_k1 = new AdjListDescriptor(k1, p1, p2, Direction.FORWARD);
        var k2 = new RelVariable("k2", 10 /*knows*/, p2, p3);
        var ald_k2 = new AdjListDescriptor(k2, p2, p3, Direction.FORWARD);

        var scan = new ScanNode(p1);
        var extend1 = new FlatExtendDefaultAdjListSingleType(ald_k1, scan.getOutSchema());
        scan.setNext(extend1);
        extend1.setPrev(scan);
        var extend2 = new FlatExtendDefaultAdjListSingleType(ald_k2, extend1.getOutSchema());
        extend1.setNext(extend2);
        extend2.setPrev(extend1);
        return wrapOperators(extend2);
    }

    private static RegularQueryPlan extend3() {
        var p1 = new NodeVariable("p1", 3 /*person*/);
        var p2 = new NodeVariable("p2", 3 /*person*/);
        var p3 = new NodeVariable("p3", 3 /*person*/);
        var p4 = new NodeVariable("p4", 3 /*person*/);
        var k1 = new RelVariable("k1", 10 /*knows*/, p1, p2);
        var ald_k1 = new AdjListDescriptor(k1, p1, p2, Direction.FORWARD);
        var k2 = new RelVariable("k2", 10 /*knows*/, p2, p3);
        var ald_k2 = new AdjListDescriptor(k2, p2, p3, Direction.FORWARD);
        var k3 = new RelVariable("k3", 10 /*knows*/, p3, p4);
        var ald_k3 = new AdjListDescriptor(k3, p3, p4, Direction.FORWARD);

        var scan = new ScanNode(p1);
        var extend1 = new FlatExtendDefaultAdjListSingleType(ald_k1, scan.getOutSchema());
        scan.setNext(extend1);
        extend1.setPrev(scan);
        var extend2 = new FlatExtendDefaultAdjListSingleType(ald_k2, extend1.getOutSchema());
        extend1.setNext(extend2);
        extend2.setPrev(extend1);
        var extend3 = new FlatExtendDefaultAdjListSingleType(ald_k3, extend2.getOutSchema());
        extend2.setNext(extend3);
        extend3.setPrev(extend2);
        return wrapOperators(extend3);
    }
}