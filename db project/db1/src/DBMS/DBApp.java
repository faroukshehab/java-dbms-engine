package DBMS;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import DBMS.Page;
import DBMS.Table;
import DBMS.FileManager;


import org.junit.Test;

public class DBApp
{
	static int dataPageSize = 6;
	
	public static void createTable(String tableName, String[] columnsNames)
	{
		Table table=new Table(tableName, columnsNames);
		FileManager.storeTable(tableName,table);
		
		
	}
	
	 public static void insert(String tableName, String[] record) {
	        
	        Table table = FileManager.loadTable(tableName);
	        ArrayList<Page> pages = table.getPages();

	        boolean inserted = false;

	       
	        for (int i = 0; i < pages.size(); i++) {
	            Page page = FileManager.loadTablePage(tableName, i);
	            if (page.getRecords().size() < dataPageSize) {
	                page.addRecord(record);
	                FileManager.storeTablePage(tableName, i, page);
	                inserted = true;
	                break;
	            }
	        }

	       
	        if (!inserted) {
	            Page newPage = new Page();
	            newPage.addRecord(record);
	            pages.add(newPage);
	            int newIndex = pages.size() - 1;
	            FileManager.storeTablePage(tableName, newIndex, newPage);
	        }

	        
	        FileManager.storeTable(tableName, table);
	    }
	
	
	
	public static ArrayList<String []> select(String tableName)
	{
		
		Table b=FileManager.loadTable(tableName);
		 ArrayList<Page> pages = b.getPages();
		 for(int i=0;i<pages.size();i++) {
			 return pages.get(i).getRecords();
		 }
		 FileManager.storeTable(tableName, b);
		 return null;
		
		
	}
	
	public static ArrayList<String []> select(String tableName, int pageNumber, int recordNumber)
	{
		Page p=FileManager.loadTablePage(tableName, pageNumber);
		ArrayList<String[]> results = new ArrayList<>();
		String[] temp= p.getRecord(recordNumber);
		results.add(temp);
		FileManager.storeTablePage(tableName, pageNumber, p);
		return results;
	}
	
	public static ArrayList<String []> select(String tableName, String[] cols, String[] vals)
	{
		Table b=FileManager.loadTable(tableName);
		String [] temp=b.getColumnsNames();
		ArrayList<Page> t=b.getPages();
		
		ArrayList<String[]> r=new ArrayList();
		for(int i=0;i<t.size();i++) {
			for(int j=0;j<temp.length;j++) {
				
				
			}
			
		}
		return null;
		
		
	}
	
	public static String getFullTrace(String tableName)
	{
		
		return FileManager.trace();
	}
	
	public static String getLastTrace(String tableName)
	{
		
		
		String  temp=getFullTrace(tableName);
		String [] t=temp.split("}, " );
		int x=t.length-1;
	    return t[x];
	    
		
		
	}	
	
	public static void main(String []args) throws IOException
	{
		


		
	

	}}
	
	
	

