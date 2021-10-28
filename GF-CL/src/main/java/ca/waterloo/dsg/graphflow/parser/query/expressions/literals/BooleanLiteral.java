package ca.waterloo.dsg.graphflow.parser.query.expressions.literals;

import ca.waterloo.dsg.graphflow.datachunk.vectors.VectorState;
import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

import java.util.Objects;

public class BooleanLiteral extends LiteralTerm {

    @Getter private final Boolean boolLiteral;

    public BooleanLiteral(Boolean boolLiteral) {
        super(boolLiteral.toString(), DataType.BOOLEAN);
        this.boolLiteral = boolLiteral;
        result = Vector.make(DataType.BOOLEAN, 1);
        result.state = VectorState.getFlatVectorState();
        result.set(0, boolLiteral);
    }

    @Override
    public Object getLiteral() {
        return boolLiteral;
    }

    @Override
    public int hashCode() {
        return super.hashCode()*31 + Objects.hash(boolLiteral);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        var otherBoolLiteral = (BooleanLiteral) o;
        return this.boolLiteral.booleanValue() == otherBoolLiteral.boolLiteral.booleanValue() &&
            this.result.getBoolean(0) == otherBoolLiteral.result.getBoolean(0);
    }
}
