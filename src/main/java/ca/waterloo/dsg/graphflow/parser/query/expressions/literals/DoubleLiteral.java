package ca.waterloo.dsg.graphflow.parser.query.expressions.literals;

import ca.waterloo.dsg.graphflow.datachunk.vectors.VectorState;
import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

import java.util.Objects;

public class DoubleLiteral extends LiteralTerm {

    @Getter private Double doubleLiteral;

    public DoubleLiteral(Double doubleLiteral) {
        super(doubleLiteral.toString(), DataType.DOUBLE);
        this.doubleLiteral = doubleLiteral;
        result = Vector.make(DataType.DOUBLE, 1);
        result.state = VectorState.getFlatVectorState();
        result.set(0, doubleLiteral);
    }

    @Override
    public Object getLiteral() {
        return doubleLiteral;
    }

    public void setNewLiteralValue(double val) {
        doubleLiteral = val;
        result.set(0, val);
    }

    @Override
    public int hashCode() {
        return super.hashCode()*31 + Objects.hash(doubleLiteral);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        var otherDoubleLiteral = (DoubleLiteral) o;
        return this.doubleLiteral.doubleValue() == otherDoubleLiteral.doubleLiteral.doubleValue() &&
            this.result.getDouble(0) == otherDoubleLiteral.result.getDouble(0);
    }
}
