package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class Table implements Serializable

{
	
	    private String tableName;
	    private String[] columnsNames;
	    private ArrayList<Page> pages;

	    public Table(String tableName, String[] columnsNames) {
	        this.tableName = tableName;
	        this.columnsNames = columnsNames;
	        this.pages = new ArrayList<>();
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
	
	

