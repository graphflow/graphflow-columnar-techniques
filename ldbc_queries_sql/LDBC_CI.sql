-- IC1

select p3.id, p3."lName", p3.birthday, p3."creationDate", p3.gender, p3."browserUsed", p3."locationIP", pl.name from person p3, "eKnows" k1, "eKnows" k2, "eKnows" k3, "eIsLocatedIn" i, place pl where k1."from" = 22468883 and k1."to" = k2."from" and k2."to" = k3."from" and k3."to" = p3.id and p3.id = i."from" and i."to" = pl.id and p3."fName" = 'Rahul';  

-- IC2

select p1.id, p1."fName", p1."lName", cmt.id, cmt.content, cmt."creationDate" from "eKnows" k1, comment cmt, "eHasCreator" hc, person p1 where k1."from" = 22468883 and k1."to" = p1.id and p1.id = hc."to" and cmt.id = hc."from" and cmt."creationDate" < 1342805711;

-- IC3

select t.name from "eKnows" k1, "eKnows" k2, post pst, "eHasCreator" hc, "eHasTag" ht, tag t where k1."from" = 22468883 and k1."from" = k2."from" and k2."to" = hc."to" and hc."from" = pst.id and pst.id = ht."from" and ht."to" = t.id and pst."creationDate" >= 1313591219 AND pst."creationDate" <= 1513591219;

-- IC4

select cmt."creationDate", cmt.content from comment cmt where cmt.id = 0;

-- IC5

select f.title from "eKnows" k1, "eKnows" k2, "eHasMember" hm, forum f, "eContainerOf" co where k1."from" = 22468883 and k1."to" = k2."from" and k2."to" = hm."to" and co."from" = f.id and f.id = hm."from" and hm.date > 1267302820;

-- IC6

select t2.name from "eKnows" k1, "eKnows" k2, "eHasCreator" hc, post pst, "eHasTag" ht1, tag t1, "eHasTag" ht2, tag t2 where k1."from" = 22468883 AND t1.name='Rumi' AND t2.name<>'Rumi' and k1."to" = k2."from" and k2."to" = hc."to" and hc."from" = pst.id and pst.id = ht1."from" and ht1."to" = t1.id and pst.id = ht2."from" and ht2."to" = t2.id;

--IC7

select p.id, p."fName", p."lName", l.date, cmt.content from comment cmt, "eHasCreator" hc, "eLikes" l, person p where hc."to" = 22468883 and cmt.id = hc."from" and cmt.id = l."to" and l."from" = p.id;

-- IC8

select p.id, p."fName", p."lName", cmt."creationDate", cmt.id, cmt.content from "eHasCreator" hc, post pst, "eReplyOf" ro, comment cmt, "eHasCreator" hc1, person p where hc."to" = 22468883 and pst.id = hc."from" and pst.id = ro."to" and ro."from" = cmt.id and cmt.id = hc1."from";

-- IC9

select p.id, p."fName", p."lName", cmt.id, cmt.content, cmt."creationDate" from "eKnows" k1, "eKnows" k2, "eHasCreator" hc, comment cmt, person p where k1."from" = 22468883 AND cmt."creationDate" < 1342840042 and k1."to" = k2."from" and k2."to" = p.id and p.id = hc."to" and hc."from" = cmt.id;

-- IC11

select p3.id, p3.fName, p3.lName, org.name from "eKnows" k1, "eKnows" k2, person p3, "eWorkAt" w, organisation o, "eIsLocatedIn" i, place pl where k1."from" = 22468883 AND w."year" < 2016 AND pl.name = 'China' and k1."to" = k2."from" and k2."to" = p3.id and p3.id = w."from" and w."to" = o.id and o.id = i."from" and i."to" = pl.id;

-- IC12

select p.id, p."fName", p."lName" from "eKnows" k1, person p, "eHasCreator" hc, "eReplyOf" ro, post pst, "eHasTag" ht, "eHasType" ty, "eIsSubclassOf" sub, tagclass tc where k1."from" = 22468883 and tc."name" = 'Person' and k1."to" = p.id and p.id = hc."to" and hc."from" = ro."from" and ro."to" = pst.id and pst.id = ht."from" and ht."to" = ty."from" and ty."to" = sub."from" and sub."to" = tc.id;

