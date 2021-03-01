package ca.waterloo.dsg.graphflow.runner;

import ca.waterloo.dsg.graphflow.parser.QueryParser;
import ca.waterloo.dsg.graphflow.parser.query.regularquery.RegularQuery;
import ca.waterloo.dsg.graphflow.planner.enumerators.RegularQueryPlanEnumerator;

import java.io.IOException;
import java.util.ArrayList;

// {forum=1, tagclass=7, post=5, person=3, organisation=2, comment=0, place=4, tag=6}

// {hasCreator=1, studyAt=13, hasTag=5, workAt=14, hasMember=3, isPartOf=8, hasModerator=4,
// hasInterest=2, isLocatedIn=7, containerOf=0, isSubclassOf=9, replyOf=12, hasType=6, knows=10,
// likes=11}

public class SinglePlanRunner extends AbstractPlanRunner {

    public static void main(String[] args) throws InterruptedException, IOException,
        ClassNotFoundException {
        var fName = "/home/p43gupta/extended/datasets/ldbc/0.1/ser-gfcore-old/";
        var stats = new ArrayList<Double>();
        loadDataset(fName, stats);
        var query = "MATCH (p:person)-[:knows]->(p1:person), (p1:person)-[:knows]->(op:person), (op:person)-[:isLocatedIn]->(city:place), (mX:comment)-[:hasCreator]->(op:person), (mx:comment)-[:isLocatedIn]->(countryX:place), (mY:comment)-[:hasCreator]->(op:person), (mY:comment)-[:isLocatedIn]->(countryY:place) WHERE p = 0 AND mX.creationDate >= 1313591219 AND mX.creationDate <= 1513591219 AND mY.creationDate >= 1313591219 AND mY.creationDate <= 1513591219 AND countryX.name = 'India' AND countryY.name = 'China' RETURN op.id, op.fName, op.lName";
        var parsedQuery = (RegularQuery) QueryParser.parseQuery(query, graph.getGraphCatalog());
        var plans =  new RegularQueryPlanEnumerator(parsedQuery, graph)
            .enumeratePlansForQuery();
        var qeStats = new ArrayList<QueryExecutionStat>();
        executeAllPlans(qeStats, plans, 0, 1);
        for (var s: qeStats) {
            System.out.println(s.toString());
        }
    }
}