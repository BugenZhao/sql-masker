select * from t where id = '23';
select * from t where id >= 1 and id <= 20;
select * from t where birth between '21-01-01' and '22-01-01';
select * from t where name = 233 and id = 233 and cash = 233 and year(last_visit) = '233';
select * from t where name between 200 and 300;
select * from t where cash >= 1.234e2;
select * from t t1, t t2 where date(t1.last_visit) = '21-10-09' and t2.name in (233, "456");
