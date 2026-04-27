package DBMS;

import java.io.Serializable;
import java.util.ArrayList;

public class Page implements Serializable
{
	  private ArrayList<String[]> records;

	    public Page() {
	        this.records = new ArrayList<>();
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
	
}
