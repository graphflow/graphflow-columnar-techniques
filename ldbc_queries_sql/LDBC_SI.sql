
--  IS1

select p."fName", p."lName", p.birthday, p."locationIP", p."browserUsed", p.gender, p."creationDate", pl.id from person p, "eIsLocatedIn" i, place pl where p.id = 22468883 and p.id = i."from" and i."to" = pl.id;	

-- IS2

select cmt.id, cmt.content, cmt."creationDate", p.id, p."fName", p."lName" from ((((("eHasCreator" hc join comment cmt on hc."from" = cmt.id) join "eReplyOf" ro on cmt.id = ro."from") join post pst on ro."to" = pst.id) join "eHasCreator" hc2 on pst.id = hc2."from") join person p on hc2."to" = p.id) where hc."to" = 22468883;

-- IS3

select p.id, p."fName", p."lName", k."date" from person p, "eKnows" k where p.id = k."to" and k."from" = 22468883;

-- IS4

select c."creationDate", c.content from comment c where c.id = 0;

-- IS5

select p.id, p."fName", p."lName" from person p, comment c, "eHasCreator" hc where c.id = 0 and c.id = hc."from" and hc."to" = p.id;

-- IS6

select f.id, f.title, p.id, p."fName", p."lName" from "eReplyOf" r, "eContainerOf" cnt, forum f, "eHasModerator" hm, person p where r."from" = 0 and r."to" = cnt."to" and f.id = cnt."from" and f.id = hm."from" and hm."to" = p.id;

-- IS7

select cmt1.id, cmt1.content, cmt1."creationDate", ra.id, ra."fName", ra."lName" from person ma, comment cmt0, "eHasCreator" hc0, "eReplyOf" ro, comment cmt1, "eHasCreator" hc1, person ra where cmt0.id = 6 and cmt0.id = hc0."from" and hc0."to" = ma.id and cmt0.id = ro."to" and ro."from" = cmt1.id and cmt1.id  = hc1."from" and hc1."to" = ra.id;

