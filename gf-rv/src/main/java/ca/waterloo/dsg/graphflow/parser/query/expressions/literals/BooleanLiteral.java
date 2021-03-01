package ca.waterloo.dsg.graphflow.parser.query.expressions.literals;

import ca.waterloo.dsg.graphflow.tuple.value.flat.BoolVal;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

import java.util.Objects;

public class BooleanLiteral extends LiteralTerm {
    @Getter private Boolean boolLiteral;

    public BooleanLiteral(Boolean boolLiteral) {
        super(boolLiteral.toString(), DataType.BOOLEAN);
        this.boolLiteral = boolLiteral;
        value = new BoolVal(boolLiteral);
    }

    @Override
    public Object getLiteral() {
        return boolLiteral;
    }

    @Override
    public int hashCode() {
        return super.hashCode()*31 + Objects.hash(boolLiteral.booleanValue());
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        var otherBoolLiteral = (BooleanLiteral) o;
        return this.boolLiteral.booleanValue() == otherBoolLiteral.boolLiteral.booleanValue() &&
            this.value.getBool() == otherBoolLiteral.value.getBool();
    }
}
