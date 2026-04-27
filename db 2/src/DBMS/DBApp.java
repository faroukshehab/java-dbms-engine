package DBMS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class DBApp {

    static int dataPageSize = 2;
     static HashMap<String, StringBuilder> traceLogs = new HashMap<>();
     static String Lasttrace;

    private static void recordTrace(String tableName, String log) {
        traceLogs.putIfAbsent(tableName, new StringBuilder());
		traceLogs.get(tableName).append(log).append("\n");
		 	
    }

    public static void createTable(String tableName, String[] columnsNames) {
    	traceLogs.remove(tableName);
        Table table = new Table(tableName, columnsNames);
        FileManager.storeTable(tableName, table);
        recordTrace(tableName, 
            "Table created name:" + tableName 
             + ", columnsNames:" + Arrays.toString(columnsNames)
        );
    }

    public static void insert(String tableName, String[] record) {
        long startTime = System.currentTimeMillis();

        try {
            Table table = FileManager.loadTable(tableName);
            ArrayList<Page> pages = table.getPages();
            boolean inserted = false;

            // Case 1: No pages exist yet
            if (pages == null || pages.isEmpty()) {
                Page newPage = new Page(dataPageSize);
                newPage.addRecord(record);
                pages = new ArrayList<>();
                pages.add(newPage);
                table.setPages(pages);
                FileManager.storeTablePage(tableName, 0, newPage);
                long executionTime = System.currentTimeMillis() - startTime;
                recordTrace(tableName,
                        "Inserted:" + Arrays.toString(record)
                                + ", at page number: 0"
                                + ", execution time (mil):" + executionTime);
            } else {
                // Case 2: Scan existing pages to find a non-full one
                for (int i = 0; i < pages.size(); i++) {
                    Page page = FileManager.loadTablePage(tableName, i);
                    if (!page.isfull()) {
                        page.addRecord(record);
                        FileManager.storeTablePage(tableName, i, page);
                        inserted = true;
                        long executionTime = System.currentTimeMillis() - startTime;
                        recordTrace(tableName,
                                "Inserted:" + Arrays.toString(record)
                                        + ", at page number:" + i
                                        + ", execution time (mil):" + executionTime);
                        break;
                    }
                }

                // Case 3: All pages are full
                if (!inserted) {
                    Page newPage = new Page(dataPageSize);
                    newPage.addRecord(record);
                    pages.add(newPage);
                    int newIndex = pages.size() - 1;
                    FileManager.storeTablePage(tableName, newIndex, newPage);
                    long executionTime = System.currentTimeMillis() - startTime;
                    recordTrace(tableName,
                            "Inserted:" + Arrays.toString(record)
                                    + ", at page number:" + newIndex
                                    + ", execution time (mil):" + executionTime);
                }
            }

            FileManager.storeTable(tableName, table);

        } catch (Exception e) {
            System.out.print("no table");;
        }
    }
    public static ArrayList<String[]> select(String tableName) {
        long startTime = System.currentTimeMillis();
        Table table = FileManager.loadTable(tableName);
        ArrayList<String[]> result = new ArrayList<>();
        for (int i = 0; i < table.getPages().size(); i++) {
            Page page = FileManager.loadTablePage(tableName, i);
            result.addAll(page.getRecords());
        }
        long executionTime = System.currentTimeMillis() - startTime;
        recordTrace(tableName, 
            "Select all pages:" + table.getPages().size() 
            + ", records:" + result.size() 
            + ", execution time (mil):" + executionTime
        );
        return result;
    }

    public static ArrayList<String[]> select(String tableName, int pageNumber, int recordNumber) {
        long startTime = System.currentTimeMillis();
        ArrayList<String[]> results = new ArrayList<>();
        Page page = FileManager.loadTablePage(tableName, pageNumber);
        if (page != null && recordNumber < page.getRecords().size()) {
            String[] record = page.getRecord(recordNumber);
            results.add(record);
        }
        long executionTime = System.currentTimeMillis() - startTime;
        recordTrace(tableName, 
            "Select pointer page:" + pageNumber 
            + ", record:" + recordNumber 
            + ", total output count:" + results.size() 
            + ", execution time (mil):" + executionTime
        );
        return results;
    }

    public static ArrayList<String[]> select(String tableName, String[] cols, String[] vals) {
        long startTime = System.currentTimeMillis();
        Table table = FileManager.loadTable(tableName);
        String[] columnNames = table.getColumnsNames();
        int[] columnIndexes = new int[cols.length];
        for (int i = 0; i < cols.length; i++) {
            for (int j = 0; j < columnNames.length; j++) {
                if (cols[i].equals(columnNames[j])) {
                    columnIndexes[i] = j;
                    break;
                }
            }
        }
        ArrayList<String[]> result = new ArrayList<>();
        ArrayList<String> perPageBreakdown = new ArrayList<>();
        int totalMatches = 0;
        for (int i = 0; i < table.getPages().size(); i++) {
            Page page = FileManager.loadTablePage(tableName, i);
            int matchesThisPage = 0;
            for (int j = 0; j < page.getRecords().size(); j++) {
                String[] record = page.getRecords().get(j);
                boolean match = true;
                for (int k = 0; k < columnIndexes.length; k++) {
                    int colIndex = columnIndexes[k];
                    if (!record[colIndex].equals(vals[k])) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    result.add(record);
                    matchesThisPage++;
                }
            }
            if (matchesThisPage > 0) {
                perPageBreakdown.add("[" + i + ", " + matchesThisPage + "]");
                totalMatches += matchesThisPage;
            }
        }
        long executionTime = System.currentTimeMillis() - startTime;
        recordTrace(tableName, 
            "Select condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals)
            + ", Records per page:" + perPageBreakdown
            + ", records:" + totalMatches
            + ", execution time (mil):" + executionTime
        );
        return result;
    }

    public static String getFullTrace(String tableName) {
        Table table = FileManager.loadTable(tableName);
        int p = table.getPages().size();
        int r = 0;
        for (int i = 0; i < p; i++) {
            Page page = FileManager.loadTablePage(tableName, i);
            r += page.getRecords().size();
        }
        String trace = traceLogs.getOrDefault(tableName, new StringBuilder()).toString();
        return trace + "Pages Count: " + p + ", Records Count: " + r;
    }

    public static String getLastTrace(String tableName) {
        String fullTrace = traceLogs.getOrDefault(tableName, new StringBuilder()).toString().trim();
        if (fullTrace.isEmpty()) {
            return "No trace available.";
        }
        String[] traces = fullTrace.split("\n");
        if (traces.length == 0) {
            return "No trace available.";
        }
        return traces[traces.length - 1];
    }

    public static void main(String[] args) throws IOException {
        String[] cols = {"id","name","major","semester","gpa"};
        createTable("student", cols);
        String[] r1 = {"1", "stud1", "CS", "5", "0.9"};
        insert("student", r1);
        String[] r2 = {"2", "stud2", "BI", "7", "1.2"};
        insert("student", r2);
        String[] r3 = {"3", "stud3", "CS", "2", "2.4"};
        insert("student", r3);
        String[] r4 = {"4", "stud4", "DMET", "9", "1.2"};
        insert("student", r4);
        String[] r5 = {"5", "stud5", "BI", "4", "3.5"};
        insert("student", r5);
        System.out.println("Output of selecting the whole table content:");
        ArrayList<String[]> result1 = select("student");
        for (String[] array : result1) {
            for (String str : array) {
                System.out.print(str + " ");
            }
            System.out.println();
        }
        System.out.println("--------------------------------");
        System.out.println("Output of selecting the output by position:");
        ArrayList<String[]> result2 = select("student", 1, 1);
        for (String[] array : result2) {
            for (String str : array) {
                System.out.print(str + " ");
            }
            System.out.println();
        }
        System.out.println("--------------------------------");
        System.out.println("Output of selecting the output by column condition:");
        ArrayList<String[]> result3 = select("student", new String[]{"gpa"}, new String[]{"1.2"});
        for (String[] array : result3) {
            for (String str : array) {
                System.out.print(str + " ");
            }
            System.out.println();
        }
        System.out.println("--------------------------------");
        System.out.println("Full Trace of the table:");
        System.out.println(getFullTrace("student"));
        System.out.println("--------------------------------");
        System.out.println("Last Trace of the table:");
        System.out.println(getLastTrace("student"));
        System.out.println("--------------------------------");
        System.out.println("The trace of the Tables Folder:");
        System.out.println(FileManager.trace());
        FileManager.reset();
        System.out.println("--------------------------------");
        System.out.println("The trace of the Tables Folder after resetting:");
        System.out.println(FileManager.trace());
    }
}