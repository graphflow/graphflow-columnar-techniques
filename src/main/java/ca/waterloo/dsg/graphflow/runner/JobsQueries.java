package ca.waterloo.dsg.graphflow.runner;

public class JobsQueries {

    private static final String Q1 =
        "MATCH (t:title)-[hc:movie_companies]->(c:company_name)," +
        "      (t:title)-[hmi:has_movie_info_idx]->(mi2:movie_info_idx) " +
        "WHERE hc.company_type_id = 2 AND mi2.info_type_id = 112 and " +
        "      hc.note contains '(co-production)'" +
        "RETURN t";
    private static final String Q2 =
        "MATCH (t:title)-[hc:movie_companies]->(c:company_name)," +
              "(t:title)-[hk:movie_keyword]->(k:keyword) " +
        "WHERE k.keyword = 'character-name-in-title' and c.country_code ='[de]' " +
        "RETURN t";
    private static final String Q3 =
        "MATCH (t:title)-[hk:movie_keyword]->(k:keyword), " +
        "      (t:title)-[hmi:has_movie_info]->(mi:movie_info) " +
        "WHERE k.keyword CONTAINS 'sequel' AND mi.info = 'Sweden' AND t.production_year > 2005 " +
        "RETURN t";
    private static final String Q4 =
        "MATCH (t:title)-[hk:movie_keyword]->(k:keyword)," +
        "      (t:title)-[hmi:has_movie_info_idx]->(mi1:movie_info_idx) " +
        "WHERE k.keyword CONTAINS 'sequel' AND mi1.info_type_id = 101 and " +
        "      mi1.info > '5.0' and t.production_year > 2005 " +
        "RETURN t";
    private static final String Q5 =
        "MATCH (t:title)-[hc:movie_companies]->(c:company_name)," +
        "      (t:title)-[hmi:has_movie_info]->(mi1:movie_info) " +
        "WHERE hc.company_type_id = 2 AND hc.note contains '(theatrical)' AND " +
            "hc.note contains '(France)' AND t.production_year > 2005 " +
        "RETURN t";
    private static final String Q6 =
        "MATCH (t:title)-[hc:cast_info]->(n:name), (t:title)-[hk:movie_keyword]->(k:keyword) " +
        "WHERE k.keyword = 'marvel-cinematic-universe' AND n.name contains 'Downey' AND " +
        "      t.production_year > 2010 " +
        "RETURN t";
    private static final String Q7 =
        "MATCH (t1:title)-[ilt:movie_link]->(t2:title), (t1:title)-[hc:cast_info]->(n:name)," +
        "      (n:name)-[han:has_aka_name]->(an:aka_name)," +
        "      (n:name)-[hpi:has_person_info]->(pi:person_info) " +
        "WHERE pi.info_type_id = 19 and an.name contains 'a' and n.name_pcode_cf >= 'A' and " +
            "n.name_pcode_cf <= 'F' and n.gender = 'm' and pi.note = 'Volker Boehm' and " +
            "t1.production_year >= 1980 and t1.production_year <= 1995 and ilt.link_type_id = 9 " +
        "RETURN t1";
    private static final String Q8 =
        "MATCH (t:title)-[hc:movie_companies]->(c:company_name), " +
        "      (t:title)-[hcs:cast_info]->(n:name), (n:name)-[han:has_aka_name]->(an:aka_name) " +
        "WHERE hcs.note ='(voice: English version)' AND c.country_code ='[jp]' AND " +
        "      hc.note contains '(Japan)' AND n.name contains 'Yo' and hcs.role_id = 2 " +
        "RETURN t";
    private static final String Q9 = "MATCH (t:title)-[hc:movie_companies]->(c:company_name), " +
        "(t:title)-[hcs:cast_info]->(n:name), (n:name)-[han:has_aka_name]->(an:aka_name)\n" +
        "  WHERE c.country_code = '[us]' AND n.gender = 'f' AND hcs.role_id = 2 AND hcs.note " +
        "starts with '(voice' and\n" +
        "                hc.note contains '(USA)' and n.name contains 'Ang' and t.production_year" +
        " >= 2005 and t.production_year <= 2015\n" +
        "  RETURN t";
    private static final String Q10 =
        "MATCH (t:title)-[hc:movie_companies]->(c:company_name), " +
        "      (t:title)-[hcs:cast_info]->(n:name) " +
        "  WHERE hcs.note contains '(voice)' and hcs.note contains '(uncredited)' and c" +
        ".country_code = '[ru]' AND hcs.role_id = 1 AND\n" +
        "                 t.production_year > 2005\n" +
        "  RETURN t";
    private static final String Q11 = "MATCH (t1:title)-[ilt:movie_link]->(t2:title), (t1:title)" +
        "-[hc:movie_companies]->(c:company_name), (t1:title)-[hk:movie_keyword]->(k:keyword)\n" +
        "  WHERE c.country_code <> '[pl]' AND c.name contains 'Film' AND hc.company_type_id = 2 " +
        "and k.keyword ='sequel' AND\n" +
        "                 ilt.link_type_id <= 2 and ilt.link_type_id >= 1 and t1.production_year " +
        "> 1950 and t1.production_year < 2000\n" +
        "  RETURN t1";
    private static final String Q12 =
        "MATCH (t:title)-[hmi1:has_movie_info]->(mi1:movie_info), (t:title)" +
            "-[hc:movie_companies]->(c:company_name),\n" +
            "                (t:title)-[hmi2:has_movie_info_idx]->(mi2:movie_info_idx)\n" +
            "  WHERE c.country_code = '[us]' and hc.company_type_id = 2 and mi1.info_type_id = 3 " +
            "and mi2.info_type_id = 101 and mi1.info = 'Drama' and\n" +
            "                mi2.info > '8.0' and t.production_year >= 2005 and t.production_year" +
            " <= 2008\n" +
            "  RETURN t";
    private static final String Q13 =
        "MATCH (t:title)-[hmi1:has_movie_info]->(mi1:movie_info), (t:title)" +
            "-[hc:movie_companies]->(c:company_name),\n" +
            "                (t:title)-[hmi2:has_movie_info_idx]->(mi2:movie_info_idx)\n" +
            "  WHERE c.country_code = '[de]' and t.kind_id = 1 and hc.company_type_id = 2 AND mi1" +
            ".info_type_id = 16 AND mi2.info_type_id = 101\n" +
            "  RETURN t";
    private static final String Q14 =
        "MATCH (t:title)-[hmi1:has_movie_info]->(mi1:movie_info), (t:title)-[hk:movie_keyword]->" +
            "(k:keyword),\n" +
            "              (t:title)-[hmi2:has_movie_info_idx]->(mi2:movie_info_idx)\n" +
            "  WHERE mi1.info_type_id = 8 AND mi2.info_type_id = 101 and k.keyword = 'murder' and" +
            " t.kind_id = 1 and mi1.info = 'USA' and\n" +
            "                mi2.info < '8.5' and t.production_year > 2010\n" +
            "  RETURN t";
    private static final String Q15 =
        "MATCH (t:title)-[hmi:has_movie_info]->(mi1:movie_info), " +
        "(t:title)-[hc:movie_companies]->(c:company_name), (t:title)-[hk:movie_keyword]->" +
        "(k:keyword)\n" +
        "  WHERE c.country_code = '[us]' and hc.note contains '(200' and hc.note contains '" +
        "(worldwide)' and mi1.info starts with 'USA:' AND\n" +
        "                mi1.note CONTAINS 'internet' AND mi1.info_type_id = 16 and t" +
        ".production_year > 2000\n" +
        "  RETURN t";
    private static final String Q16 =
        "MATCH (t:title)-[hk:movie_keyword]->(k:keyword), (t:title)-[hc:movie_companies]->" +
            "(c:company_name), (t:title)-[hcs:cast_info]->(n:name),\n" +
            "               (n:name)-[han:has_aka_name]->(an:aka_name)\n" +
            "  WHERE c.country_code = '[us]' and k.keyword = 'character-name-in-title' and t" +
            ".episode_nr >= 50 and t.episode_nr < 100\n" +
            "  RETURN t";
    private static final String Q17 =
        "MATCH (t:title)-[hcs:cast_info]->(n:name), (t:title)-[hc:movie_companies]->" +
            "(c:company_name), (t:title)-[hk:movie_keyword]->(k:keyword)\n" +
            "  WHERE c.country_code ='[us]' and  k.keyword ='character-name-in-title' and n.name " +
            "starts with 'B'\n" +
            "  RETURN t";
    private static final String Q18 =
        "MATCH (t:title)-[hmi1:has_movie_info]->(mi1:movie_info), (t:title)" +
            "-[hmi2:has_movie_info_idx]->(mi2:movie_info_idx),\n" +
            "                (t:title)-[hcs:cast_info]->(n:name)\n" +
            "  WHERE mi1.info_type_id = 105 and mi2.info_type_id = 100 and n.gender = 'm' and n" +
            ".name contains 'Tim'\n" +
            "  RETURN t";
    private static final String Q19 =
        "MATCH (t:title)-[hmi:has_movie_info]->(mi1:movie_info), (t:title)-[hc:movie_companies]->" +
            "(c:company_name), (t:title)-[hcs:cast_info]->(n:name),\n" +
            "                (n:name)-[han:has_aka_name]->(an:aka_name)\n" +
            "  WHERE c.country_code = '[us]' AND n.gender = 'f' AND mi1.info_type_id = 16 AND hcs" +
            ".role_id = 2 and hcs.note starts with '(voice' and\n" +
            "                hc.note contains '(USA)' and mi1.info starts with 'Japan:' and n" +
            ".name contains 'Ang' and t.production_year >= 2005 and\n" +
            "                t.production_year <= 2009\n" +
            "  RETURN t";
    private static final String Q20 =
        "MATCH (t:title)-[hk:movie_keyword]->(k:keyword), (t:title)-[hci:has_complete_cast]->" +
            "(ci:complete_cast), (t:title)-[hcs:cast_info]->(n:name)\n" +
            "  WHERE t.kind_id = 1 AND ci.subject_id = 1 AND ci.status_id >= 3 and k.keyword = " +
            "'superhero' and t.production_year > 1950 and\n" +
            "                hcs.name contains 'Tony' and hcs.name contains 'Stark'\n" +
            "  RETURN t";
    private static final String Q21 =
        "MATCH (t:title)-[hmi:has_movie_info]->(mi1:movie_info), (t:title)-[hc:movie_companies]->" +
            "(c:company_name), (t:title)-[hk:movie_keyword]->(k:keyword),\n" +
            "                (t:title)-[ilt:movie_link]->(t2:title)\n" +
            "  WHERE c.country_code <> '[pl]' AND c.name CONTAINS 'Film' AND hc.company_type_id =" +
            " 2 AND k.keyword CONTAINS 'sequel' AND\n" +
            "                ilt.link_type_id <= 2 and mi1.info = 'Germany' and t.production_year" +
            " >= 1950 and t.production_year <= 2000\n" +
            "  RETURN t";
    private static final String Q22 =
        "MATCH (t:title)-[hmi1:has_movie_info]->(mi1:movie_info), (t:title)" +
            "-[hmi2:has_movie_info_idx]->(mi2:movie_info_idx),\n" +
            "               (t:title)-[hc:movie_companies]->(c:company_name), (t:title)" +
            "-[hk:movie_keyword]->(k:keyword)\n" +
            "  WHERE c.country_code <> '[us]' AND mi1.info_type_id = 8 AND mi2.info_type_id = 101" +
            " AND k.keyword = 'murder' and t.kind_id = 1 and\n" +
            "                 hc.note contains '(200' and mi1.info = 'USA' and mi2.info < '7.0' " +
            "and t.production_year > 2008\n" +
            "  RETURN t";
    private static final String Q23 =
        "MATCH (t:title)-[hmi:has_movie_info]->(mi1:movie_info), (t:title)-[hc:movie_companies]->" +
            "(c:company_name),\n" +
            "                (t:title)-[hk:movie_keyword]->(k:keyword), (t:title)" +
            "-[hci:has_complete_cast]->(ci:complete_cast)\n" +
            "  WHERE c.country_code = '[us]' AND ci.status_id = 4 AND mi1.info_type_id = 16 AND " +
            "mi1.note contains 'internet' and\n" +
            "                t.kind_id = 1 and mi1.info starts with 'USA:' and t.production_year " +
            "> 2000\n" +
            "  RETURN t";
    private static final String Q24 =
        "MATCH (t:title)-[hmi:has_movie_info]->(mi1:movie_info)," +
        "      (t:title)-[hc:movie_companies]->(c:company_name)," +
        "      (t:title)-[hcs:cast_info]->(n:name), (n:name)-[han:has_aka_name]->(an:aka_name)," +
        "      (t:title)-[hk:movie_keyword]->(k:keyword) " +
        "WHERE hcs.note starts with '(voice:' AND c.country_code = '[us]' AND " +
            "mi1.info_type_id = 16 and k.keyword = 'hero' AND mi1.info starts with 'USA:' and " +
            "n.gender = 'f' AND hcs.role_id = 2 AND t.production_year > 2010 " +
        "RETURN t";
    private static final String Q25 = "MATCH (t:title)-[hmi1:has_movie_info]->(mi1:movie_info), " +
        "(t:title)-[hmi2:has_movie_info_idx]->(mi2:movie_info_idx),\n" +
        "                (t:title)-[hk:movie_keyword]->(k:keyword), (t:title)-[hcs:cast_info]->" +
        "(n:name)\n" +
        "  WHERE  mi1.info_type_id = 3 and mi2.info_type_id = 100 and k.keyword = 'murder' and " +
        "mi1.info = 'Horror' and\n" +
        "                 n.gender = 'm'\n" +
        "  RETURN t";
    private static final String Q26 =
        "MATCH (t:title)-[hmi2:has_movie_info_idx]->(mi2:movie_info_idx), (t:title)" +
            "-[hk:movie_keyword]->(k:keyword),\n" +
            "                (t:title)-[hcs:cast_info]->(n:name), (t:title)" +
            "-[hci:has_complete_cast]->(ci:complete_cast)\n" +
            "  WHERE ci.subject_id = 1 and ci.status_id >= 3 and hcs.name contains 'man' and mi2" +
            ".info_type_id = 101 and\n" +
            "                 k.keyword = 'superhero' and t.kind_id = 1 and mi2.info > '7.0' and " +
            "t.production_year > 2000\n" +
            "  RETURN t";
    private static final String Q27 =
        "MATCH (t1:title)-[hmi1:has_movie_info]->(mi1:movie_info), (t1:title)" +
            "-[hk:movie_keyword]->(k:keyword),\n" +
            "                (t1:title)-[ilt:movie_link]->(t2:title), (t1:title)" +
            "-[hc:movie_companies]->(c:company_name),\n" +
            "                (t1:title)-[hci:has_complete_cast]->(ci:complete_cast)\n" +
            "  WHERE ci.subject_id <= 2 and ci.status_id = 3 and c.country_code <> '[pl]' and c" +
            ".name contains 'Film' and\n" +
            "                hc.company_type_id = 2 and k.keyword ='sequel' and ilt.link_type_id " +
            "<= 2 and mi1.info = 'Sweden' and\n" +
            "                t1.production_year >= 1950 AND t1.production_year <= 2000\n" +
            "  RETURN t1";
    private static final String Q28 =
        "MATCH (t:title)-[hmi1:has_movie_info]->(mi1:movie_info), (t:title)" +
            "-[hmi2:has_movie_info_idx]->(mi2:movie_info_idx),\n" +
            "               (t:title)-[hk:movie_keyword]->(k:keyword), (t:title)" +
            "-[hc:movie_companies]->(c:company_name),\n" +
            "               (t:title)-[hci:has_complete_cast]->(ci:complete_cast)\n" +
            "  WHERE ci.subject_id = 2 and ci.status_id <> 4 and c.country_code <> '[us]' and mi1" +
            ".info_type_id = 8 and\n" +
            "                mi2.info_type_id = 101 and k.keyword = 'murder' and t.kind_id = 1 " +
            "and hc.note contains '(200' and\n" +
            "                mi1.info = 'Germany' and mi2.info < '8.5' and t.production_year > " +
            "2000\n" +
            "  RETURN t";
    private static final String Q29 =
        "MATCH (t:title)-[hmi1:has_movie_info]->(mi1:movie_info), " +
        "      (t:title)-[hk:movie_keyword]->(k:keyword), " +
        "      (t:title)-[hci:has_complete_cast]->(ci:complete_cast), " +
        "      (t:title)-[hcs:cast_info]->(n:name)," +
        "      (n:name)-[han:has_aka_name]->(an:aka_name)," +
        "      (n:name)-[hpi:has_person_info]->(pi:person_info), " +
        "      (t:title)-[hc:movie_companies]->(c:company_name) " +
        "WHERE ci.subject_id =2 and ci.status_id = 4 and hcs.name = 'Queen' and " +
        "      hcs.note contains '(voice' and c.country_code ='[us]' and mi1.info_type_id = 16 and " +
        "      pi.info_type_id = 17 and k.keyword = 'computer-animation' and " +
        "      mi1.info starts with 'Japan:' and n.gender = 'f' and n.name contains 'An' and " +
        "      hcs.role_id = 2 and t.title = 'Shrek 2' and t.production_year >= 2000 AND " +
        "      t.production_year <= 2010 " +
        "RETURN t";
    private static final String Q30 =
        "MATCH (t:title)-[hmi1:has_movie_info]->(mi1:movie_info), (t:title)" +
            "-[hmi2:has_movie_info_idx]->(mi2:movie_info_idx),\n" +
            "               (t:title)-[hk:movie_keyword]->(k:keyword), (t:title)" +
            "-[hcs:cast_info]->(n:name),\n" +
            "               (t:title)-[hci:has_complete_cast]->(ci:complete_cast)\n" +
            "  WHERE ci.subject_id <= 2 and ci.status_id = 4 and mi1.info_type_id = 3 and mi2" +
            ".info_type_id = 100 and\n" +
            "                 k.keyword = 'murder' and mi1.info = 'Horror' and n.gender = 'm' and" +
            " t.production_year > 2000\n" +
            "  RETURN t";
    private static final String Q31 =
        "MATCH (t:title)-[hmi1:has_movie_info]->(mi1:movie_info), (t:title)" +
            "-[hmi2:has_movie_info_idx]->(mi2:movie_info_idx),\n" +
            "                (t:title)-[hk:movie_keyword]->(k:keyword), (t:title)" +
            "-[hcs:cast_info]->(n:name),\n" +
            "                (t:title)-[hc:movie_companies]->(c:company_name)\n" +
            "  WHERE mi1.info_type_id = 3 and mi2.info_type_id = 100 and k.keyword = 'murder' and" +
            " mi1.info = 'Horror' and\n" +
            "                n.gender = 'm'\n" +
            "  RETURN t";
    private static final String Q32 =
        "MATCH (t:title)-[hk:movie_keyword]->(k:keyword), (t:title)-[ilt:movie_link]->(t2:title)" +
            "\n" +
            "  WHERE k.keyword ='character-name-in-title'\n" +
            "  RETURN t";
    private static final String Q33 =
        "MATCH (t1:title)-[ilt:movie_link]->(t2:title), (t1:title)-[hmi21:has_movie_info_idx]->" +
            "(mi21:movie_info_idx),\n" +
            "                (t2:title)-[hmi22:has_movie_info_idx]->(mi22:movie_info_idx), " +
            "(t1:title)-[hc1:movie_companies]->(c1:company_name),\n" +
            "                (t2:title)-[hc2:movie_companies]->(c2:company_name)\n" +
            "  WHERE c1.country_code = '[us]' and mi21.info_type_id = 101 and mi22.info_type_id =" +
            " 101 and t1.kind_id = 2 and\n" +
            "                t2.kind_id = 2 and ilt.link_type_id <= 2 and mi22.info < '3.0' and\n" +
            "                t2.production_year >= 2005 and t2.production_year <= 2008\n" +
            "  RETURN t1";

    public static String[] queries = {
        Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q13, Q14, Q15, Q16, Q17, Q18, Q19, Q20,
        Q21, Q22, Q23, Q24, Q25, Q26, Q27, Q28, Q29, Q30, Q31, Q32, Q33
    };

}
