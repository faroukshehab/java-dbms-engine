package DBMS;

import java.io.Serializable;
import java.util.ArrayList;

public class Page implements Serializable
{
	  private ArrayList<String[]> records;
	  static int  dataPageSize;

	    public Page(int dataPageSize) {
	        this.records = new ArrayList<>();
	        this.dataPageSize= dataPageSize;
	    }

	    public ArrayList<String[]> getRecords() {
	        return records;
	    }

	    public void addRecord(String[] record) {
	        records.add(record);
	    }

	    public String[] getRecord(int index) {
	        return records.get(index);
	    }

	    public int getSize() {
	        return records.size();
	    }
	    public boolean isfull() {
	    	if(this.getSize()< dataPageSize) {
	    		return false;
	    	}
	    	else
	    		return true;
	    }
	
}
