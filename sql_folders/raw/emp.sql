create or replace temporary table temp1
as
select emp_name, emp_gmail_id from p1
union all
select emp_name, emp_gmail_id from p2
union all
select emp_name, emp_gmail_id from p3
; 

MERGE INTO emp as t
USING temp1 as s
ON t.emp_name=s.emp_name
WHEN MATCHED THEN
   UPDATE SET 
   t.emp_gmail_id=s.emp_gmail_id
WHEN NOT MATCHED THEN
   INSERT (t.emp_name,t.emp_gmail_id)
   VALUES (s.emp_name,s.emp_gmail_id);
