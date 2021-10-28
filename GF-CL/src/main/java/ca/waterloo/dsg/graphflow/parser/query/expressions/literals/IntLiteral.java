package ca.waterloo.dsg.graphflow.parser.query.expressions.literals;

import ca.waterloo.dsg.graphflow.datachunk.vectors.VectorState;
import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

import java.util.Objects;

public class IntLiteral extends LiteralTerm {

    @Getter private int intLiteral;

    public IntLiteral(int intLiteral) {
        super(intLiteral + "", DataType.INT);
        this.intLiteral = intLiteral;
        result = Vector.make(DataType.INT, 1);
        result.state = VectorState.getFlatVectorState();
        result.set(0, intLiteral);
    }

    public void setNewLiteralValue(int val) {
        intLiteral = val;
        result.set(0, intLiteral);
    }

    @Override
    public Object getLiteral() {
        return intLiteral;
    }

    @Override
    public int hashCode() {
        return super.hashCode()*31 + Objects.hash(intLiteral);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        var otherLongLiteral = (IntLiteral) o;
        return this.intLiteral == otherLongLiteral.intLiteral &&
            this.result.getInt(0) == otherLongLiteral.result.getInt(0);
    }
}
