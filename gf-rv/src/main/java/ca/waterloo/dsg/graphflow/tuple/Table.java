package ca.waterloo.dsg.graphflow.tuple;

import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.SimpleVariable;
import ca.waterloo.dsg.graphflow.tuple.value.Value;
import ca.waterloo.dsg.graphflow.tuple.value.flat.IntVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.NodeVal;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import com.google.common.collect.HashMultiset;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

public class Table {
    @Getter private List<Tuple> tuples;
    @Getter private Tuple sampleTupleForSchema;

    public Table(Tuple sampleTupleForSchema) {
        this.sampleTupleForSchema = sampleTupleForSchema;
        this.tuples = new ArrayList<>();
    }

    public void add(Tuple tuple) {
        if (!sampleTupleForSchema.hasSameSchema(tuple)) {
            throw new IllegalArgumentException("Cannot add into a table a tuple with a different " +
                "schema than the schema of the table. \nTable schema: "
                + sampleTupleForSchema.schemaAsStr() + "\nTuple schema: " + tuple.schemaAsStr());
        }
        tuples.add(tuple.deepCopy());
    }

    public void printTable() {
        System.out.println(sampleTupleForSchema.schemaAsStr());
        var vals = new String[sampleTupleForSchema.numValues()];
        for (var tuple : tuples) {
            for (int j = 0; j < tuple.numValues(); ++j) {
                vals[j] = tuple.get(j).getValAsStr();
            }
            System.out.printf(sampleTupleForSchema.getPrintFormat(), (Object[]) vals);
            System.out.println();
        }
        System.out.println("NUM TUPLES PRINTED: " + tuples.size());
    }

    public boolean hasSameSchema(Table ot) {
        return this.sampleTupleForSchema.hasSameSchema(ot.sampleTupleForSchema);
    }

    /**
     * This method is oblivious to the ordering of columns, that is
     * if two tables have same column A and B, but one of table has A, B but another one has B, A,
     * it will return true.
     */
    public boolean isSame(Table ot) {
        var a = 0;
        if (!hasSameSchema(ot)) return false;
        if (ot.tuples.size() != this.tuples.size()) return false;
        var m1 = HashMultiset.create();
        var m2 = HashMultiset.create();
        var encode = Base64.getEncoder();
        var schema = this.sampleTupleForSchema.getSchema();
        var tupleItr = this.tuples.iterator();
        var otTupleItr = ot.tuples.iterator();
        while (tupleItr.hasNext()) {
            var t1 = tupleItr.next();
            var t2 = otTupleItr.next();
            if (t1.numValues() != t2.numValues()) return false;
            var varNames = schema.getVarNames();
            varNames.sort(String::compareTo);
        }
        return m1.equals(m2);
    }

    public boolean isSameAndInSameOrder(Table ot) {
        if (!hasSameSchema(ot)) return false;
        if (ot.tuples.size() != this.tuples.size()) return false;
        var m1 = new ArrayList<String>();
        var m2 = new ArrayList<String>();
        var encode = Base64.getEncoder();
        var schema = this.sampleTupleForSchema.getSchema();
        var tupleItr = this.tuples.iterator();
        var otTupleItr = ot.tuples.iterator();
        while (tupleItr.hasNext()) {
            var t1 = tupleItr.next();
            var t2 = otTupleItr.next();
            if (t1.numValues() != t2.numValues()) return false;
        }
        return m1.equals(m2);
    }

    public static Table constructTable(Tuple sampleTuple, List<int[]> values) {
        var table = new Table(sampleTuple);
        for (var tupleLongValue : values) {
            assert sampleTuple.numValues() == tupleLongValue.length;
            var tuple = new Tuple();
            for (var i = 0; i < tupleLongValue.length; i++) {
                var val = sampleTuple.get(i);
                switch (val.getDataType()) {
                    case NODE:
                        tuple.append(new NodeVal(val.getVariableName(), val.getNodeType(),
                            tupleLongValue[i]), new NodeVariable(val.getVariableName(),
                            val.getNodeType()));
                        break;
                    case INT:
                        tuple.append(new IntVal(val.getVariableName(), tupleLongValue[i]),
                            new SimpleVariable(val.getVariableName(), DataType.INT));
                        break;
                }
            }
            table.add(tuple);
        }
        return table;
    }

    /**
     * WARNING: There are issues with this function:
     * 1. It uses the incorrect version of append() {which has to be removed}
     * 2. It is replication of above function and is used only in GroupByAggregateTests
     * */
    public static Table constructTableForTest(Tuple sampleTuple, List<Value[]> values) {
        var table = new Table(sampleTuple);
        for (var tupleValue : values) {
            assert sampleTuple.numValues() == tupleValue.length;
            var tuple = new Tuple();
            for (var i = 0; i < tupleValue.length; ++i) {
                tuple.append(tupleValue[i]);
            }
            table.add(tuple);
        }
        return table;
    }

    public void removeDuplicates() {
        this.tuples = tuples.stream().distinct().collect(Collectors.toList());
    }
}
