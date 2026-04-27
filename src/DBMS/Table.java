package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class Table implements Serializable

{
	
	    private String tableName;
	    private String[] columnsNames;
	    private ArrayList<Page> pages;
	    HashMap<String, StringBuilder> traceLogs = new HashMap<>();

	    public Table(String tableName, String[] columnsNames) {
	        this.tableName = tableName;
	        this.columnsNames = columnsNames;
	        this.pages = new ArrayList<>();
	    }
	    public void setPages(ArrayList<Page> pages) {
	        this.pages = pages;
	    }

	    public String getTableName() {
	        return tableName;
	    }

	    public String[] getColumnsNames() {
	        return columnsNames;
	    }

	    public ArrayList<Page> getPages() {
	        return pages;
	    }

	    public void addPage(Page page) {
	        pages.add(page);
	    }

	    public Page getPage(int index) {
	        return pages.get(index);
	    }
	}
	
	

