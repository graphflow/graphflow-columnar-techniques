package ca.waterloo.dsg.graphflow.parser.query.expressions.literals;

import ca.waterloo.dsg.graphflow.datachunk.vectors.VectorState;
import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

public class StringLiteral extends LiteralTerm {

    @Getter private String stringLiteral;

    public StringLiteral(String stringLiteral) {
        super(stringLiteral, DataType.STRING);
        this.stringLiteral = stringLiteral;
        result = Vector.make(DataType.STRING, 1);
        result.state = VectorState.getFlatVectorState();
        result.set(0, stringLiteral);
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
            this.result.getString(0).equals(otherStringLiteral.result.getString(0));
    }
}
