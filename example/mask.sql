select * from t where birth between '21-01-01' and '22-01-01';
select * from t where name = 233 and id = 233;
select * from t where name between 200 and 300;
select * from t where cash >= 1.234e2;
select * from t where id = '23'; -- point get
select * from t where date(last_visit) = '21-10-09' and name in (select name from t where cash >= 1.234e2);
select * from t where id = '23' and name = 233; -- selection with child (point get)
