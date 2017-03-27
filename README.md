# Hbase Operations Java Implementation
This repository has a java class that implements java hbase operations.
1)	Create a table named emp in HBase with two column families:companyinfo, personalinfo
hbase> create 'emp','companyinfo','personalinfo'
2) We will need these columns in companyinfo : deptno 
3) We will need these columns in personalinfo : fname,lname,salary,dob
4) Key is empid(example : "emp1")
5) This class does these operations in Hbase table
6) addRecord:  Put (or insert) a row
7) delRecord: Delete a row
8) getOneRecord:Get a row
9) printAllRowsWithScan:Get all rows by using Scan class object
10) printAllRowsWithGet:Get all rows with Get method. This uses getOneRecord method in loop
11) getOneRecordObject:Get a row object. This is used to get the who row object as Key Value Map. This is used in           sumSalaryEmployeeOfSameDept mehtod. 
12)  sumSalaryEmployeeOfSameDept: Get sum of salary of two employees of same department

