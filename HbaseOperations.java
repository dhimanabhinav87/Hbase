package com.adhiman.hbase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

public class HbaseOperations {

    // Instantiating Configuration class
    Configuration config = HBaseConfiguration.create();
    
    /**
     * Put (or insert) a row
     */
    public void addRecord(String tableName, String rowKey,String family, String key, String value) throws Exception {
        try {
            HTable table = new HTable(config, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Delete a row
     */
    public void delRecord(String tableName, String rowKey)
            throws IOException {
        HTable table = new HTable(config, tableName);
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        table.close();
        System.out.println("del recored " + rowKey + " ok.");
    }
 
   
    /**
     * Get a row
     */
    public void getOneRecord (String tableName, String rowKey) throws IOException{
        HTable table = new HTable(config, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for(KeyValue kv : rs.raw()){
            System.out.print(new String(kv.getRow()) + " " );
            System.out.print(new String(kv.getFamily()) + ":" );
            System.out.print(new String(kv.getQualifier()) + " " );
            System.out.print(kv.getTimestamp() + " " );
            System.out.println(new String(kv.getValue()));
            table.close();
        }
    }
    
    /**
     * Get a row object
     */
    public Map<String, String> getOneRecordObject (String tableName, String rowKey) throws IOException{
        HTable table = new HTable(config, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        Map<String, String> hbaserecord = new HashMap<String,String>();
        
        for(KeyValue kv : rs.raw()){
        	hbaserecord.put(new String(kv.getQualifier()), new String(kv.getValue()));
           }
        
        table.close();
        return hbaserecord;
    }
    
    /**
     * Get all rows with Scan
     */
	public void printAllRowsWithScan(String tableName) {
		try{
            HTable table = new HTable(config, tableName);
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            for(Result r:ss){
                for(KeyValue kv : r.raw()){
                   System.out.print(new String(kv.getRow()) + " ");
                   System.out.print(new String(kv.getFamily()) + ":");
                   System.out.print(new String(kv.getQualifier()) + " ");
                   System.out.print(kv.getTimestamp() + " ");
                   System.out.println(new String(kv.getValue()));
                  
                }
            }
            table.close();
       } catch (IOException e){
           e.printStackTrace();
       }
		
	}

	/**
     * Get all rows with Get
     */
	public void printAllRowsWithGet(String tableName) throws IOException {
		HTable table = new HTable(config, tableName.getBytes());
		System.out.println("scanning full table:");
		ResultScanner scanner = table.getScanner(new Scan());
		for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
			  byte[] key = rr.getRow();
			  this.getOneRecord(tableName, new String(key));
			 
			}
		table.close();
	}
	/**
     * Get sum of salary of two employees of same department
     */
	public void sumSalaryEmployeeOfSameDept(String tableName, String empKey1, String empKey2) {
		//get dept from empkey1 and dept from empkey2
		//if dep1==dept2 then  get salary and add salary and return else they don't belong to same dept
		Map<String, String> hbaseGetRecord1;
		Map<String, String> hbaseGetRecord2;
		try {
			hbaseGetRecord1 = (Map<String, String>) this.getOneRecordObject(tableName, empKey1);
			hbaseGetRecord2 = (Map<String, String>) this.getOneRecordObject(tableName, empKey2);
			String dept1=null;
			String dept2=null;
			String salary1=null;
			String salary2=null;
			 Iterator it = hbaseGetRecord1.entrySet().iterator();
			    while (it.hasNext()) {
			        Map.Entry pair = (Map.Entry)it.next();
			        if(pair.getKey().equals("deptno")){
			        	dept1=(String) pair.getValue();
			        }
			        if(pair.getKey().equals("salary")){
			        	salary1=(String) pair.getValue();
			        }
			    }
			  
			    Iterator it2 = hbaseGetRecord2.entrySet().iterator();
			    while (it2.hasNext()) {
			        Map.Entry pair2 = (Map.Entry)it2.next();
			        if(pair2.getKey().equals("deptno")){
			        	dept2=(String) pair2.getValue();
			        }
			        if(pair2.getKey().equals("salary")){
			        	salary2=(String) pair2.getValue();
			        }
			    }
			    
			    if((dept1.equals(dept2))){
			    	int TotalSalary= Integer.parseInt(salary1)+Integer.parseInt(salary2);
			    	System.out.println("Department number  is : "+dept1);
			    	System.out.println("Sum of salaries is : "+TotalSalary);
			    }
			    else {
			    	System.out.println("Department of two employees are not same");
			    }
			   
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}


}
