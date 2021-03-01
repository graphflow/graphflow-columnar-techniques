package ca.waterloo.dsg.graphflow.parser.query.expressions.literals;

import ca.waterloo.dsg.graphflow.tuple.value.flat.IntVal;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

import java.util.Objects;

public class IntLiteral extends LiteralTerm {
    @Getter private int intLiteral;

    public IntLiteral(int intLiteral) {
        super(intLiteral + "", DataType.INT);
        this.intLiteral = intLiteral;
        value = new IntVal(intLiteral);
    }

    public void setNewLiteralValue(int val) {
        intLiteral = val;
        value.setInt(val);
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
            this.value.getInt() == otherLongLiteral.value.getInt();
    }
}
