package ca.waterloo.dsg.graphflow.runner;

public class Query {
    public String name;
    public String qstr;
    public String[] qvo;

    public Query(String name, String str, String[] QVO) {
        this.name = name;
        this.qstr = str;
        this.qvo = QVO;
    }

    public String printQVO() {
        var a = new StringBuilder();
        a.append("[ ");
        for (var i = 0; i < qvo.length; i++) {
            a.append(qvo[i]);
            if (i < qvo.length - 1) {
                a.append(" , ");
            }
        }
        a.append(" ]");
        return a.toString();
    }
}