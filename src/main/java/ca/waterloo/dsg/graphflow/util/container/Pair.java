package ca.waterloo.dsg.graphflow.util.container;

import java.io.Serializable;

/**
 * A mutable Pair (A a, B b).
 */
public class Pair<A, B> implements Serializable {

    public A a;
    public B b;

    public Pair(A a, B b) {
        this.a = a;
        this.b = b;
    }
}
