package ca.waterloo.dsg.graphflow.runner;

public class LDBCQueries {

    public static class Query {
        public String name;
        public String str;
        public String[] QVO;

        public Query(String name, String str, String[] QVO) {
            this.name = name;
            this.str = str;
            this.QVO = QVO;
        }
    }

    public static final Query[] queries = new Query[] {
        new Query("IC11",
            "MATCH (p1:Person)-[:knows]->(p2:Person),(p2:Person)-[:knows]->(p3:Person)," +
                "      (p3:Person)-[e:workAt]->(org:Organisation)," +
                "      (org:Organisation)-[:isLocatedIn]->(pl:Place) " +
                "WHERE p1.cid < 22477075 and p3.cid < 22477075 and e.year < 2016 AND pl.name = 'China' " +
                "RETURN p3, p3.fName, p3.lName, org.name",
            new String[]{"p1", "p2", "p3", "org", "pl"}),

        new Query("IC06",
            "MATCH (p1:Person)-[:knows]->(p2:Person),(p2:Person)-[:knows]->(p3:Person)," +
                "      (post:Post)-[:hasCreator]->(p3:Person)," +
                "      (post:Post)-[:hasTag]->(tag:Tag)," +
                "      (post:Post)-[:hasTag]->(otherTag:Tag) " +
                "WHERE p1.cid < 22477075 AND p3.cid <= 22468893 and tag.name='Rumi' AND otherTag" +
                ".name<>'Rumi' " +
                "RETURN otherTag.name",
            new String[]{"p1", "p2", "p3", "post", "tag", "otherTag"}),

        new Query("IC05",
            "MATCH (p1:Person)-[:knows]->(p2:Person),(p2:Person)-[:knows]->(p3:Person)," +
                "      (f:Forum)-[e:hasMember]->(p3:Person),(f:Forum)-[:containerOf]->(p:Post) " +
                "WHERE p1.cid < 22477075 AND p3.cid <= 22468893 and e.date > 1267302820 " +
                "RETURN f.title",
            new String[]{"p1", "p2", "p3", "f", "p"}),

        new Query("IC03",
            "MATCH (person:Person)-[:knows]->(p1:Person),(p1:Person)-[:knows]->(op:Person)," +
                "      (op:Person)-[:isLocatedIn]->(city:Place)," +
                "      (mX:Comment)-[:hasCreator]->(op:Person)," +
                "      (mX:Comment)-[:isLocatedIn]->(countryX:Place)," +
                "      (mY:Comment)-[:hasCreator]->(op:Person)," +
                "      (mY:Comment)-[:isLocatedIn]->(countryY:Place) " +
                "WHERE person.cid < 22477075 and op.cid <= 22468893 AND " +
                "      mX.creationDate >= 1313591219 AND mX.creationDate <= 1513591219 AND " +
                "      mY.creationDate >= 1313591219 AND mY.creationDate <= 1513591219 AND " +
                "      countryX.name = 'India' AND countryY.name = 'China' " +
                "RETURN op, op.fName, op.lName",
            new String[]{"person", "p1", "op", "city", "mX", "countryX", "mY", "countryY"}),

        new Query("IC03_1",
            "MATCH (person:Person)-[:knows]->(p1:Person),(p1:Person)-[:knows]->(op:Person)," +
                "      (op:Person)-[:isLocatedIn]->(city:Place)," +
                "      (mX:Comment)-[:hasCreator]->(op:Person)," +
                "      (mX:Comment)-[:isLocatedIn]->(countryX:Place)," +
                "      (mY:Comment)-[:hasCreator]->(op:Person)," +
                "      (mY:Comment)-[:isLocatedIn]->(countryY:Place) " +
                "WHERE person.cid < 22477075 and op.cid <= 22468893 AND " +
                "      mX.creationDate >= 1313591219 AND mX.creationDate <= 1513591219 AND " +
                "      mY.creationDate >= 1313591219 AND mY.creationDate <= 1513591219 " +
                "RETURN op, op.fName, op.lName",
            new String[]{"person", "p1", "op", "city", "mX", "countryX", "mY", "countryY"}),

        new Query("IC07",
            "MATCH (friend:Person)-[e:likes]->(cmt:Comment)," +
                "      (cmt:Comment)-[:hasCreator]->(person:Person) " +
                "WHERE person.cid < 22477075 and friend.cid <= 22468893 " +
                "RETURN friend, friend.fName, friend.lName, e.date, cmt.content",
            new String[]{"person", "cmt", "friend"}),

        new Query("IC09",
            "MATCH (p1:Person)-[:knows]->(p2:Person),(p2:Person)-[:knows]->(p3:Person)," +
                "      (cmt:Comment)-[:hasCreator]->(p3:Person) " +
                "WHERE p1.cid < 22477075 and p3.cid < 22477075 and cmt.creationDate < 1342840042 " +
                "RETURN p3.fName, p3.lName, cmt, cmt.content, cmt.creationDate",
            new String[]{"p3", "cmt", "p2", "p1"}),

        new Query("IC01",
            "MATCH (person:Person)-[:knows]->(p1:Person),(p1:Person)-[:knows]->(p2:Person)," +
                "      (p2:Person)-[:knows]->(op:Person),(op:Person)-[:isLocatedIn]->(city:Place) " +
                "WHERE person.cid < 22485267 and op.cid <= 22468893 " +
                "RETURN op, op.lName, op.birthday, op.creationDate, op.gender, op.locationIP, " +
                "       city.name",
            new String[] {"person", "p1", "p2", "op", "city"}),

        new Query("IS06",
            "MATCH (c:Comment)-[:replyOf]->(pst:Post)," +
                "      (f:Forum)-[:containerOf]->(pst:Post)," +
                "      (f:Forum)-[:hasModerator]->(p:Person) " +
                "WHERE c.cid < 65536 " +
                "RETURN f, f.title, p, p.fName, p.lName",
            new String[] {"c", "pst", "f", "p"}),

        new Query("IS06_1",
            "MATCH (c:Comment)-[:replyOf]->(pst:Post)," +
                "      (f:Forum)-[:containerOf]->(pst:Post)," +
                "      (f:Forum)-[:hasModerator]->(p:Person) " +
                "WHERE c.cid < 524288 " +
                "RETURN f, f.title, p, p.fName, p.lName",
            new String[] {"c", "pst", "f", "p"}),

        new Query("IS02",
        "MATCH (c:Comment)-[:hasCreator]->(p:Person), (c:Comment)-[:replyOf]->(post:Post), " +
            "      (post:Post)-[:hasCreator]->(op:Person) " +
            "WHERE p.cid < 22498884 and op.cid < 22478884 " +
            "RETURN c, c.content, c.creationDate, op, op.fName, op.lName",
        new String[] {"p", "c", "post", "op"}),

        new Query("IS07",
            "MATCH (cmt1:Comment)-[:replyOf]->(cmt0:Comment), " +
                "      (cmt0:Comment)-[:hasCreator]->(msgAuthor:Person), " +
                "      (cmt1:Comment)-[:hasCreator]->(rplyAuthor:Person) " +
                "WHERE cmt0.cid < 65536 " +
                "RETURN cmt1, cmt1.content, cmt1.creationDate, rplyAuthor, rplyAuthor.fName, " +
                "       rplyAuthor.lName",
            new String[] {"cmt0", "msgAuthor", "cmt1", "rplyAuthor"}),

        new Query("IS07_1",
            "MATCH (cmt1:Comment)-[:replyOf]->(cmt0:Comment), " +
                "      (cmt0:Comment)-[:hasCreator]->(msgAuthor:Person), " +
                "      (cmt1:Comment)-[:hasCreator]->(rplyAuthor:Person) " +
                "WHERE cmt0.cid < 524288 " +
                "RETURN cmt1, cmt1.content, cmt1.creationDate, rplyAuthor, rplyAuthor.fName, " +
                "       rplyAuthor.lName",
            new String[] {"cmt0", "msgAuthor", "cmt1", "rplyAuthor"}),

        new Query("IS05",
            "MATCH (comment:Comment)-[:hasCreator]->(person:Person) " +
                "WHERE comment.cid < 65536 " +
                "RETURN person, person.fName, person.lName",
            new String[] {"comment", "person"}),

        new Query("IS05",
            "MATCH (comment:Comment)-[:hasCreator]->(person:Person) " +
                "WHERE comment.cid < 524288 " +
                "RETURN person, person.fName, person.lName",
            new String[] {"comment", "person"}),

        new Query("IS04",
            "MATCH (comment:Comment)-[:hasCreator]->(person:Person) WHERE comment.cid = 0 RETURN " +
                "comment.creationDate, comment.content",
            new String[] {"comment", "person"}),

        new Query("IS01",
            "MATCH (p:Person)-[:isLocatedIn]->(pl:Place) " +
            "WHERE p.cid < 22534528 " +
            "RETURN p.fName, p.lName, p.birthday, p.locationIP, p.browserUsed, p.gender, " +
            "       p.creationDate, pl",
            new String[] {"p", "pl"}),

        new Query("IS01_1",
            "MATCH (p:Person)-[:isLocatedIn]->(pl:Place) " +
                "WHERE p.cid < 22468884 " +
                "RETURN p.fName, p.lName, p.birthday, p.locationIP, p.browserUsed, p.gender, " +
                "       p.creationDate, pl",
            new String[] {"p", "pl"}),

        new Query("IS02",
            "MATCH (c:Comment)-[:hasCreator]->(p:Person), (c:Comment)-[:replyOf]->(post:Post), " +
            "      (post:Post)-[:hasCreator]->(op:Person) " +
            "WHERE p.cid < 22534528 " +
            "RETURN c, c.content, c.creationDate, op, op.fName, op.lName",
            new String[] {"p", "c", "post", "op"}),

        new Query("IS02_1",
            "MATCH (c:Comment)-[:hasCreator]->(p:Person), (c:Comment)-[:replyOf]->(post:Post), " +
                "      (post:Post)-[:hasCreator]->(op:Person) " +
                "WHERE p.cid < 22468884 " +
                "RETURN c, c.content, c.creationDate, op, op.fName, op.lName",
            new String[] {"p", "c", "post", "op"}),

        new Query("IS03",
            "MATCH (person:Person)-[e:knows]->(friend:Person) " +
            "WHERE person.cid < 22534528 " +
            "RETURN friend, friend.fName, friend.lName, e.date",
            new String[] {"person", "friend"}),

        new Query("IS03_1",
            "MATCH (person:Person)-[e:knows]->(friend:Person) " +
                "WHERE person.cid < 22468884 " +
                "RETURN friend, friend.fName, friend.lName, e.date",
            new String[] {"person", "friend"}),

        new Query("IS05",
            "MATCH (comment:Comment)-[:hasCreator]->(person:Person) " +
            "WHERE comment.cid < 21865434 " +
            "RETURN person, person.fName, person.lName",
            new String[] {"comment", "person"}),

        new Query("IS05_1",
            "MATCH (comment:Comment)-[:hasCreator]->(person:Person) " +
                "WHERE comment.cid < 1 " +
                "RETURN person, person.fName, person.lName",
            new String[] {"comment", "person"}),

        new Query("IS06",
            "MATCH (c:Comment)-[:replyOf]->(pst:Post)," +
            "      (f:Forum)-[:containerOf]->(pst:Post)," +
            "      (f:Forum)-[:hasModerator]->(p:Person) " +
            "WHERE c.cid < 21865434 " +
            "RETURN f, f.title, p, p.fName, p.lName",
            new String[] {"c", "pst", "f", "p"}),

        new Query("IS06_1",
            "MATCH (c:Comment)-[:replyOf]->(pst:Post)," +
                "      (f:Forum)-[:containerOf]->(pst:Post)," +
                "      (f:Forum)-[:hasModerator]->(p:Person) " +
                "WHERE c.cid < 1 " +
                "RETURN f, f.title, p, p.fName, p.lName",
            new String[] {"c", "pst", "f", "p"}),

        new Query("IS07",
            "MATCH (cmt1:Comment)-[:replyOf]->(cmt0:Comment), " +
            "      (cmt0:Comment)-[:hasCreator]->(msgAuthor:Person), " +
            "      (cmt1:Comment)-[:hasCreator]->(rplyAuthor:Person) " +
            "WHERE cmt0.cid < 21865434 " +
            "RETURN cmt1, cmt1.content, cmt1.creationDate, rplyAuthor, rplyAuthor.fName, " +
            "       rplyAuthor.lName",
            new String[] {"cmt0", "msgAuthor", "cmt1", "rplyAuthor"}),

        new Query("IS07_1",
            "MATCH (cmt1:Comment)-[:replyOf]->(cmt0:Comment), " +
                "      (cmt0:Comment)-[:hasCreator]->(msgAuthor:Person), " +
                "      (cmt1:Comment)-[:hasCreator]->(rplyAuthor:Person) " +
                "WHERE cmt0.cid < 1 " +
                "RETURN cmt1, cmt1.content, cmt1.creationDate, rplyAuthor, rplyAuthor.fName, " +
                "       rplyAuthor.lName",
            new String[] {"cmt0", "msgAuthor", "cmt1", "rplyAuthor"}),

        new Query("IC02",
            "MATCH (person:Person)-[:knows]->(friend:Person)," +
            "      (message:Comment)-[:hasCreator]->(friend:Person) " +
            "WHERE person.cid < 22477075 and message.creationDate < 1342805711 " +
            "RETURN friend, friend.fName, friend.lName, message, message.content, message.creationDate",
            new String[]{"person", "friend", "message"}),

        new Query("IC03",
            "MATCH (person:Person)-[:knows]->(p1:Person),(p1:Person)-[:knows]->(op:Person)," +
            "      (op:Person)-[:isLocatedIn]->(city:Place)," +
            "      (mX:Comment)-[:hasCreator]->(op:Person)," +
            "      (mX:Comment)-[:isLocatedIn]->(countryX:Place)," +
            "      (mY:Comment)-[:hasCreator]->(op:Person)," +
            "      (mY:Comment)-[:isLocatedIn]->(countryY:Place) " +
            "WHERE person.cid < 22477075 and" +
            "      mX.creationDate >= 1313591219 AND mX.creationDate <= 1513591219 AND " +
            "      mY.creationDate >= 1313591219 AND mY.creationDate <= 1513591219 AND " +
            "      countryX.name = 'India' AND countryY.name = 'China' " +
            "RETURN op, op.fName, op.lName",
            new String[]{"person", "p1", "op", "city", "mX", "countryX", "mY", "countryY"}),

        new Query("IC04",
            "MATCH (person:Person)-[:knows]->(friend:Person)," +
            "      (person:Person)-[:knows]->(friend2:Person)," +
            "      (post:Post)-[:hasCreator]->(friend:Person)," +
            "      (post:Post)-[:hasTag]->(tag:Tag) " +
            "WHERE person.cid < 22477075 and post.creationDate >= 1313591219 AND" +
            "      post.creationDate <= 1513591219 " +
            "RETURN tag.name",
            new String[]{"person", "friend", "post", "tag", "friend2"}),

        new Query("IC05",
            "MATCH (p1:Person)-[:knows]->(p2:Person),(p2:Person)-[:knows]->(p3:Person)," +
            "      (f:Forum)-[e:hasMember]->(p3:Person),(f:Forum)-[:containerOf]->(p:Post) " +
            "WHERE p1.cid < 22477075 AND p3.cid <= 22468893 and e.date > 1267302820 " +
            "RETURN f.title",
            new String[]{"p1", "p2", "p3", "f", "p"}),

        new Query("IC06",
            "MATCH (p1:Person)-[:knows]->(p2:Person),(p2:Person)-[:knows]->(p3:Person)," +
            "      (post:Post)-[:hasCreator]->(p3:Person)," +
            "      (post:Post)-[:hasTag]->(tag:Tag)," +
            "      (post:Post)-[:hasTag]->(otherTag:Tag) " +
            "WHERE p1.cid < 22477075 AND p3.cid <= 22468893 and tag.name='Rumi' AND otherTag" +
                ".name<>'Rumi' " +
            "RETURN otherTag.name",
            new String[]{"p1", "p2", "p3", "post", "tag", "otherTag"}),

        new Query("IC07",
            "MATCH (friend:Person)-[e:likes]->(cmt:Comment)," +
            "      (cmt:Comment)-[:hasCreator]->(person:Person) " +
            "WHERE person.cid < 22477075 " +
            "RETURN friend, friend.fName, friend.lName, e.date, cmt.content",
            new String[]{"person", "cmt", "friend"}),

        new Query("IC08",
            "MATCH (cmt:Comment)-[:replyOf]->(pst:Post),(pst:Post)-[:hasCreator]->(p:Person)," +
            "      (cmt:Comment)-[:hasCreator]->(cmtAuthor:Person) " +
            "WHERE p.cid < 22477075 " +
            "RETURN cmtAuthor, cmtAuthor.fName, cmtAuthor.lName, cmt.creationDate, cmt, " +
            "       cmt.content",
            new String[]{"p", "pst", "cmt", "cmtAuthor"}),

        new Query("IC09",
            "MATCH (p1:Person)-[:knows]->(p2:Person),(p2:Person)-[:knows]->(p3:Person)," +
            "      (cmt:Comment)-[:hasCreator]->(p3:Person) " +
            "WHERE p3.cid < 22477075 and cmt.creationDate < 1342840042 " +
            "RETURN p3.fName, p3.lName, cmt, cmt.content, cmt.creationDate",
            new String[]{"p3", "cmt", "p2", "p1"}),

        new Query("IC11",
            "MATCH (p1:Person)-[:knows]->(p2:Person),(p2:Person)-[:knows]->(p3:Person)," +
            "      (p3:Person)-[e:workAt]->(org:Organisation)," +
            "      (org:Organisation)-[:isLocatedIn]->(pl:Place) " +
            "WHERE p1.cid < 22477075 and e.year < 2016 AND pl.name = 'China' " +
            "RETURN p3, p3.fName, p3.lName, org.name",
            new String[]{"p1", "p2", "p3", "org", "pl"}),

        new Query("IC12",
            "MATCH (person:Person)-[:knows]->(friend:Person)," +
            "      (c:Comment)-[:hasCreator]->(friend:Person)," +
            "      (c:Comment)-[:replyOf]->(pst:Post),(pst:Post)-[:hasTag]->(tag:Tag)," +
            "      (tag:Tag)-[:hasType]->(tg:TagClass)," +
            "      (tg:TagClass)-[:isSubclassOf]->(tagClass:TagClass) " +
            "WHERE person.cid < 22477075 AND tagClass.name='Person' " +
            "RETURN friend, friend.fName, friend.lName",
            new String[]{"person", "friend", "c", "pst", "tag", "tg", "tagClass"}),
    };
}
