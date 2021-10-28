package ca.waterloo.dsg.graphflow.parser.query.expressions.literals;

import ca.waterloo.dsg.graphflow.tuple.value.flat.StringVal;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

public class StringLiteral extends LiteralTerm {
    @Getter private String stringLiteral;

    public StringLiteral(String stringLiteral) {
        super(stringLiteral, DataType.STRING);
        this.stringLiteral = stringLiteral;
        value = new StringVal(stringLiteral);
    }

    @Override
    public Object getLiteral() {
        return stringLiteral;
    }

    @Override
    public int hashCode() {
        return super.hashCode()*31 + stringLiteral.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        var otherStringLiteral = (StringLiteral) o;
        return this.stringLiteral.equals(otherStringLiteral.stringLiteral) &&
            this.value.getString() == otherStringLiteral.value.getString();
    }
}
