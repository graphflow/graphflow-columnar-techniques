package ca.waterloo.dsg.graphflow.parser.query.expressions.literals;

import ca.waterloo.dsg.graphflow.tuple.value.flat.DoubleVal;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

import java.util.Objects;

public class DoubleLiteral extends LiteralTerm {
    @Getter private Double doubleLiteral;

    public DoubleLiteral(Double doubleLiteral) {
        super(doubleLiteral.toString(), DataType.DOUBLE);
        this.doubleLiteral = doubleLiteral;
        value = new DoubleVal(doubleLiteral);
    }

    @Override
    public Object getLiteral() {
        return doubleLiteral;
    }

    public void setNewLiteralValue(double val) {
        doubleLiteral = val;
        value.setDouble(val);
    }

    @Override
    public int hashCode() {
        return super.hashCode()*31 + Objects.hash(doubleLiteral.doubleValue());
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        var otherDoubleLiteral = (DoubleLiteral) o;
        return this.doubleLiteral.doubleValue() == otherDoubleLiteral.doubleLiteral.doubleValue() &&
            this.value.getDouble() == otherDoubleLiteral.value.getDouble();
    }
}
