package ca.waterloo.dsg.graphflow.storage.properties.relpropertystore.relpropertylists;

import lombok.Getter;

import java.io.IOException;
import java.io.Serializable;

public abstract class RelPropertyList implements Serializable {

    @Getter protected int relLabel;
    @Getter protected int nodeType;

    public RelPropertyList(int relLabel, int nodeType) {
        this.relLabel = relLabel;
        this.nodeType = nodeType;
    }

    public abstract void serialize(String directory) throws IOException;
}
