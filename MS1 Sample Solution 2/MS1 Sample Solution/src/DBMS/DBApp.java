package DBMS;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBApp
{
	static int dataPageSize = 2;


	public static void createTable(String tableName, String[] columnsNames)
	{
		Table t = new Table(tableName, columnsNames);
		FileManager.storeTable(tableName, t);
	}

	public static void insert(String tableName, String[] record)
	{
		Table t = FileManager.loadTable(tableName);
		t.insert(record);
		FileManager.storeTable(tableName, t);
	}

	public static ArrayList<String []> select(String tableName)
	{
		Table t = FileManager.loadTable(tableName);
		ArrayList<String []> res = t.select();
		FileManager.storeTable(tableName, t);
		return res;
	}

	public static ArrayList<String []> select(String tableName, int pageNumber, int recordNumber)
	{
		Table t = FileManager.loadTable(tableName);
		ArrayList<String []> res = t.select(pageNumber, recordNumber);
		FileManager.storeTable(tableName, t);
		return res;
	}

	public static ArrayList<String []> select(String tableName, String[] cols, String[] vals)
	{
		Table t = FileManager.loadTable(tableName);
		ArrayList<String []> res = t.select(cols, vals);
		FileManager.storeTable(tableName, t);
		return res;
	}

	public static String getFullTrace(String tableName)
	{
		Table t = FileManager.loadTable(tableName);
		String res = t.getFullTrace();
		return res;
	}

	public static String getLastTrace(String tableName)
	{
		Table t = FileManager.loadTable(tableName);
		String res = t.getLastTrace();
		return res;
	}
	
	
	public static ArrayList<String []> validateRecords( String tableName){
		Table t = FileManager.loadTable(tableName);
		if (t == null) {
			System.out.println("Table not found during validation: " + tableName);
			return new ArrayList<>();
		}

		String fullTraceLog = getFullTrace(tableName);
		ArrayList<String[]> recordsPotentiallyMissing = new ArrayList<>();
		ArrayList<String> allInsertionLogs = new ArrayList<>();

		// Pattern to capture record data and page number from "Inserted" log entries
		// Example: "Inserted record: [val1, val2] in page number: X, record number: Y"
		Pattern insertionPattern = Pattern.compile("Inserted record: \\[(.*?)\\] in page number: (\\d+),");

		for (String logEntry : fullTraceLog.split("\n")) {
			if (logEntry.contains("Inserted record:")) { // Pre-filter to reduce regex operations
				allInsertionLogs.add(logEntry);
			}
		}

		ArrayList<Integer> actualStoredPageIds = getPagesFromTrace(tableName);

		for (String insertionLog : allInsertionLogs) {
			Matcher matcher = insertionPattern.matcher(insertionLog);
			if (matcher.find()) {
				String recordContentString = matcher.group(1); // Content within brackets
				String pageNumString = matcher.group(2);       // Page number

				try {
					int loggedPageNum = Integer.parseInt(pageNumString.trim());
					if (!actualStoredPageIds.contains(loggedPageNum)) {
						// Page mentioned in log doesn't exist on disk, so record is missing
						String[] recordValues = recordContentString.split(",\\s*");
						recordsPotentiallyMissing.add(recordValues);
					}
				} catch (NumberFormatException e) {
					System.err.println("Could not parse page number from log entry: " + insertionLog);
				}
			}
		}
		
		t.getTrace().add("Validation check: Identified " + recordsPotentiallyMissing.size() + " missing records based on trace.");
		FileManager.storeTable(tableName, t);

		return recordsPotentiallyMissing;
	}
	
	
	
		
	public static ArrayList<Integer> getPagesFromTrace(String tableName) {
	    ArrayList<Integer> foundPageNumbers = new ArrayList<>();
	    File tableDirectory = new File(FileManager.directory.getAbsolutePath() + File.separator + tableName);

	    if (!tableDirectory.exists() || !tableDirectory.isDirectory()) {
	        return foundPageNumbers; // Return empty list if directory doesn't exist
	    }

	    File[] pageFiles = tableDirectory.listFiles(); // Get all files/dirs

	    if (pageFiles == null) {
	        return foundPageNumbers; // Return empty if error listing files
	    }

	    for (File file : pageFiles) {
	        if (file.isFile() && file.getName().endsWith(".db")) {
	            String fileNameWithoutExtension = file.getName().substring(0, file.getName().lastIndexOf(".db"));
	            try {
	                int pageNumber = Integer.parseInt(fileNameWithoutExtension);
	                foundPageNumbers.add(pageNumber);
	            } catch (NumberFormatException e) {
	                // Log or handle error if filename isn't a valid page number, e.g., "metadata.db"
	                 System.err.println("Skipping non-numeric page file: " + file.getName());
	            }
	        }
	    }
	    Collections.sort(foundPageNumbers); // Optional: keep them sorted for consistency
	    return foundPageNumbers;
	}

	
	
	
	
	
	
	
	
	
	
//	public static void recoverRecords(String tableName, ArrayList<String[]> missingRecordsToRecover) {
//	    Table t = FileManager.loadTable(tableName);
//	    if (t == null) {
//	        System.err.println("Table " + tableName + " not found for recovery.");
//	        return;
//	    }
//
//	    ArrayList<Integer> existingPageIds = getPagesFromTrace(tableName);
//	    ArrayList<Integer> newlyCreatedPageIds = new ArrayList<>();
//	    int totalExpectedPages = t.getPageCount(); // Total pages table *thinks* it has
//	    int numberOfRecordsRecovered = 0;
//
//	    // Use a mutable copy if the original list needs to be preserved,
//	    // or if an iterator is preferred for removal.
//	    ArrayList<String[]> recordsQueue = new ArrayList<>(missingRecordsToRecover);
//	    Iterator<String[]> recordsIterator = recordsQueue.iterator();
//
//	    // Iterate through all page slots table schema knows about
//	    for (int pageIdx = 0; pageIdx < totalExpectedPages; pageIdx++) {
//	        if (!existingPageIds.contains(pageIdx)) {
//	            // This page is missing, try to recover it
//	            Page newPage = new Page();
//	            int recordsInCurrentNewPage = 0;
//	            
//	            while (recordsInCurrentNewPage < dataPageSize && recordsIterator.hasNext()) {
//	                String[] recordToInsert = recordsIterator.next();
//	                newPage.insert(recordToInsert);
//	                numberOfRecordsRecovered++;
//	                recordsInCurrentNewPage++;
//	                // recordsIterator.remove(); // Remove from recordsQueue if it's being consumed exclusively here.
//	                                        // If missingRecordsToRecover is the source and should be emptied, iterate it directly.
//	                                        // Current approach: iterate a copy, original missingRecordsToRecover is untouched by iterator.
//	            }
//	            
//	            if (recordsInCurrentNewPage > 0) { // Only store if page has records
//	                FileManager.storeTablePage(tableName, pageIdx, newPage);
//	                newlyCreatedPageIds.add(pageIdx);
//	            }
//	            
//	            if (!recordsIterator.hasNext() && recordsInCurrentNewPage == 0 && newPage.getRecords().isEmpty()) {
//	                // No more records to recover, and this potential page slot remained empty
//	                // No need to store an empty page if no records were assigned to it.
//	            }
//	        }
//	    }
//	    
//	    // If there are still records left but no more "known" page slots from t.getPageCount()
//	    // This implies an issue with pageCount or more missing records than slots.
//	    // The current logic only fills up to t.getPageCount().
//	    // For records remaining beyond that, new pages would need to be appended, 
//	    // potentially increasing t.getPageCount(). This is beyond the original scope.
//
//	    t.getTrace().add("Recovery process: " + numberOfRecordsRecovered + " records placed into pages: " + newlyCreatedPageIds.toString() + ".");
//	    FileManager.storeTable(tableName, t);
//	}

	
	
	
	
	
	
	
	public static void recoverRecords(String tableName, ArrayList<String[]> missingRecordsToRecover) {
	    Table t = FileManager.loadTable(tableName);
	    if (t == null) {
	        System.err.println("Table " + tableName + " not found for recovery.");
	        return;
	    }

	    ArrayList<Integer> existingPageIds = getPagesFromTrace(tableName);
	    ArrayList<Integer> newlyCreatedPageIds = new ArrayList<>();
	    int totalPotentialPages = t.getPageCount(); // Max page index based on table metadata
	    int numberOfRecordsRecovered = 0;
	    
	    int currentMissingRecordIdx = 0; // Index to iterate through missingRecordsToRecover

	    for (int pageIdx = 0; pageIdx < totalPotentialPages; pageIdx++) {
	        if (!existingPageIds.contains(pageIdx)) {
	            // This page is missing, try to recover it
	            Page newPageToCreate = new Page();
	            int recordsAddedToThisPage = 0;
	            
	            while (recordsAddedToThisPage < dataPageSize && currentMissingRecordIdx < missingRecordsToRecover.size()) {
	                String[] recordToInsert = missingRecordsToRecover.get(currentMissingRecordIdx);
	                newPageToCreate.insert(recordToInsert);
	                
	                numberOfRecordsRecovered++;
	                recordsAddedToThisPage++;
	                currentMissingRecordIdx++; // Move to the next missing record
	            }
	            
	            if (recordsAddedToThisPage > 0) { // Only store if the page received records
	                FileManager.storeTablePage(tableName, pageIdx, newPageToCreate);
	                newlyCreatedPageIds.add(pageIdx);
	            }
	            
	            if (currentMissingRecordIdx >= missingRecordsToRecover.size()) {
	                // All missing records have been processed
	                break; 
	            }
	        }
	    }
	    
	    // Handle remaining records if all page slots up to t.getPageCount() are filled or existing
	    // This part would require creating new pages beyond t.getPageCount() and updating table metadata.
	    // For simplicity and matching original apparent scope, we'll assume records fit into missing slots.
	    // If currentMissingRecordIdx < missingRecordsToRecover.size(), some records couldn't be placed.

	    t.getTrace().add("Recovery process: Attempted to place " + missingRecordsToRecover.size() + " records. " 
	                     + numberOfRecordsRecovered + " records placed into pages: " + newlyCreatedPageIds.toString() + ".");
	    FileManager.storeTable(tableName, t);
	}
	
	
	
	
	
	
	
	


	public static void createBitMapIndex(String tableName, String colName) {
		long executionStartTime = System.currentTimeMillis();
		Table t = FileManager.loadTable(tableName);
		if (t == null) {
			System.err.println("Table " + tableName + " not found for bitmap index creation.");
			return;
		}

		String[] tableColumns = t.getColumnsNames();
		int targetColumnIndex = -1;
		for (int i = 0; i < tableColumns.length; i++) {
			if (tableColumns[i].equals(colName)) {
				targetColumnIndex = i;
				break;
			}
		}

		if (targetColumnIndex == -1) {
			System.err.println("Column '" + colName + "' not found in table '" + tableName + "'. Index not created.");
			// Potentially add a trace to the table object about this failure
			// t.getTrace().add("Failed to create index: Column " + colName + " not found.");
			// FileManager.storeTable(tableName, t);
			return;
		}

		ArrayList<String[]> allTableRecords = new ArrayList<>();
		Set<String> distinctValuesInColumn = new HashSet<>();
		
		int numPages = t.getPageCount();
		for (int i = 0; i < numPages; i++) {
			Page currentPage = FileManager.loadTablePage(tableName, i);
			if (currentPage != null) {
				for (String[] record : currentPage.getRecords()) {
					allTableRecords.add(record);
					if (record.length > targetColumnIndex) { // Ensure record has the column
						distinctValuesInColumn.add(record[targetColumnIndex]);
					}
				}
			}
		}

		// Convert set of unique values to a sorted list for consistent bitmap construction
		ArrayList<String> sortedUniqueValues = new ArrayList<>(distinctValuesInColumn);
		Collections.sort(sortedUniqueValues); // Sorting ensures consistent order if it matters

		BitmapIndex newBitmapIndex = new BitmapIndex();

		// For each record, and for each unique value, determine the bit.
		// This builds all bitmaps (one for each unique value) simultaneously.
		for (String[] record : allTableRecords) {
			String actualValueInRecord = (record.length > targetColumnIndex) ? record[targetColumnIndex] : null;
			for (String uniqueVal : sortedUniqueValues) {
				if (uniqueVal.equals(actualValueInRecord)) {
					newBitmapIndex.add(uniqueVal, 1);
				} else {
					newBitmapIndex.add(uniqueVal, 0);
				}
			}
		}

		FileManager.storeTableIndex(tableName, colName, newBitmapIndex);
		long executionEndTime = System.currentTimeMillis();
		t.getTrace().add("Bitmap index created for column: " + colName + ". Execution time (ms): " + (executionEndTime - executionStartTime));
		FileManager.storeTable(tableName, t);
	}
	
	
	
	
	
	
	

	public static String getValueBits(String tableName, String colName, String value) {
	    BitmapIndex columnIndex = FileManager.loadTableIndex(tableName, colName);

	    if (columnIndex == null || columnIndex.getIndex() == null) {
	        // Consider if table should be loaded to log trace for this event
	        return "Index not found for " + tableName + "." + colName;
	    }

	    ArrayList<Integer> bitList = columnIndex.getIndex().get(value);

	    if (bitList == null) { // Value does not exist in the index
	        int bitmapLength = 0;
	        // Determine the expected length of bitmaps from any existing entry
	        if (!columnIndex.getIndex().isEmpty()) {
	            // Get the first available bit list to determine the common length
	            ArrayList<Integer> anyEntryBitList = columnIndex.getIndex().values().iterator().next();
	            if (anyEntryBitList != null) {
	                bitmapLength = anyEntryBitList.size();
	            }
	        }
	        // Return a string of all zeros of the determined length
	        StringBuilder zeroBitmap = new StringBuilder(bitmapLength);
	        for (int i = 0; i < bitmapLength; i++) {
	            zeroBitmap.append('0');
	        }
	        return zeroBitmap.toString();
	    }

	    // Value exists, construct its bit string
	    StringBuilder bitStreamBuilder = new StringBuilder(bitList.size());
	    for (Integer bit : bitList) {
	        bitStreamBuilder.append(bit);
	    }
	    return bitStreamBuilder.toString();
	}
	

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public static ArrayList<String []> selectIndex(String tableName,String[] queryConditionCols, String[] queryConditionVals){
		if (queryConditionCols.length != queryConditionVals.length){
			System.err.println("Mismatch between condition columns and values count.");
			return new ArrayList<>(); // Return empty for malformed query
		}
		
		Table t = FileManager.loadTable(tableName);
		if (t == null) {
		    System.err.println("Table " + tableName + " not found for indexed select.");
		    return new ArrayList<>();
		}

		ArrayList<String[]> allRecordsInTable = getallrecords(tableName); // Potentially large, loaded upfront
		ArrayList<String[]> resultSet = new ArrayList<>();
		  
		List<String> indexedQueryCols = new ArrayList<>();
		List<String> nonIndexedQueryCols = new ArrayList<>();
		  
		for (String colName : queryConditionCols){
		    BitmapIndex index = FileManager.loadTableIndex(tableName, colName);
		    if (index != null){
		        indexedQueryCols.add(colName);
		    } else {
		        nonIndexedQueryCols.add(colName);
		    }
	    }
		
		long startTime = System.currentTimeMillis();
		ArrayList<String[]> intermediateResults = new ArrayList<>();

		if (!indexedQueryCols.isEmpty()) {
		    // Step 1: Filter using indexed columns
		    List<String> bitmapsForIndexedCols = new ArrayList<>();
		    for (String indexedCol : indexedQueryCols) {
		        // Find the corresponding value for this indexedCol from queryConditionVals
		        String valueForCol = "";
		        for(int i=0; i < queryConditionCols.length; i++){
		            if(queryConditionCols[i].equals(indexedCol)){
		                valueForCol = queryConditionVals[i];
		                break;
		            }
		        }
		        bitmapsForIndexedCols.add(getValueBits(tableName, indexedCol, valueForCol));
		    }

		    if (bitmapsForIndexedCols.isEmpty() || bitmapsForIndexedCols.get(0).isEmpty()) {
		        // If no bitmaps or first bitmap is empty, means no records can match index part
		        // or an error occurred. If no indexed columns were actually processed, this path might be taken.
		        // This check might need refinement based on getValueBits behavior for empty/error.
		        // For now, assume if bitmapsForIndexedCols is populated, they are valid.
		    }
            
            // Combine bitmaps using AND
            int[] finalCombinedBitmap = null;
            if (!bitmapsForIndexedCols.isEmpty()) {
                finalCombinedBitmap = bitmapToIntArray(bitmapsForIndexedCols.get(0));
                for (int i = 1; i < bitmapsForIndexedCols.size(); i++) {
                    int[] currentBitmapArray = bitmapToIntArray(bitmapsForIndexedCols.get(i));
                    finalCombinedBitmap = bitwiseAnd(finalCombinedBitmap, currentBitmapArray);
                }
            }


		    // Populate intermediateResults based on combined bitmap
		    if (finalCombinedBitmap != null) {
		        for (int i = 0; i < finalCombinedBitmap.length && i < allRecordsInTable.size(); i++){
		            if (finalCombinedBitmap[i] == 1){
		                intermediateResults.add(allRecordsInTable.get(i));
		            }
		        }
		    } else if (indexedQueryCols.isEmpty()) { // No indexed columns were used, so all records are candidates for non-indexed filtering
                intermediateResults.addAll(allRecordsInTable);
            }


		    // Step 2: Filter intermediateResults using non-indexed columns (if any)
		    if (!nonIndexedQueryCols.isEmpty()) {
		        resultSet = filterRecordsByNonIndexedConditions(t, intermediateResults, nonIndexedQueryCols, queryConditionCols, queryConditionVals);
		    } else {
		        resultSet.addAll(intermediateResults); // All conditions were indexed, or no non-indexed conditions
		    }

		} else { // No indexed columns in query at all
		    // Fallback to sequential scan for all conditions
		    // Or use the filterRecordsByNonIndexedConditions on allTableRecords
            // The original code calls t.select(cols, vals) which is a full scan
            System.out.println("No indexed columns found in query. Performing non-indexed select for all conditions.");
		    resultSet = t.select(queryConditionCols, queryConditionVals); // Using the table's own select method
		}
		
		long stopTime = System.currentTimeMillis();
		
		// Prepare for trace message
		Collections.sort(indexedQueryCols);
		Collections.sort(nonIndexedQueryCols);
		
		String traceMessage;
		if (!indexedQueryCols.isEmpty() && !nonIndexedQueryCols.isEmpty()) { // Mixed case
		    traceMessage = String.format("Select index condition:%s->%s, Indexed columns: %s, Indexed selection count: %d, Non Indexed columns: %s, Final count: %d, execution time (mil):%d",
		        Arrays.toString(queryConditionCols), Arrays.toString(queryConditionVals), indexedQueryCols.toString(),
		        intermediateResults.size(), nonIndexedQueryCols.toString(), resultSet.size(), (stopTime-startTime));
		} else if (!indexedQueryCols.isEmpty()) { // All indexed
		    traceMessage = String.format("Select index condition:%s->%s, Indexed columns: %s, Indexed selection count: %d, Final count: %d, execution time (mil):%d",
		        Arrays.toString(queryConditionCols), Arrays.toString(queryConditionVals), indexedQueryCols.toString(),
		        intermediateResults.size(), resultSet.size(), (stopTime-startTime));
		} else { // All non-indexed (fallback)
		    List<String> sortedQueryCols = new ArrayList<>(Arrays.asList(queryConditionCols));
		    Collections.sort(sortedQueryCols);
		    traceMessage = String.format("Select index condition:%s->%s, Non Indexed columns: %s, Final count: %d, execution time (mil):%d",
		        Arrays.toString(queryConditionCols), Arrays.toString(queryConditionVals), sortedQueryCols.toString(),
		        resultSet.size(), (stopTime-startTime));
		}
		t.getTrace().add(traceMessage);
		FileManager.storeTable(tableName, t);
		return resultSet;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	private static ArrayList<String[]> filterRecordsByNonIndexedConditions(
	        Table table,
	        ArrayList<String[]> recordsToFilter,
	        List<String> nonIndexedConditionCols, // Columns from query that are not indexed
	        String[] originalQueryCols,           // All columns in the original SELECT query
	        String[] originalQueryVals) {         // All values in the original SELECT query

	    ArrayList<String[]> finalFilteredResults = new ArrayList<>();
	    String[] tableSchema = table.getColumnsNames();

	    // Pre-calculate mappings for faster lookups
	    Map<String, Integer> tableColNameToIndexMap = new HashMap<>();
	    for (int i = 0; i < tableSchema.length; i++) {
	        tableColNameToIndexMap.put(tableSchema[i], i);
	    }

	    Map<String, String> queryColToValueMap = new HashMap<>();
	    for (int i = 0; i < originalQueryCols.length; i++) {
	        queryColToValueMap.put(originalQueryCols[i], originalQueryVals[i]);
	    }

	    for (String[] record : recordsToFilter) {
	        boolean matchesAllNonIndexed = true;
	        for (String nonIndexedColName : nonIndexedConditionCols) {
	            Integer recordColIdx = tableColNameToIndexMap.get(nonIndexedColName);
	            String expectedValue = queryColToValueMap.get(nonIndexedColName);

	            if (recordColIdx == null || expectedValue == null) { // Should not happen if inputs are valid
	                matchesAllNonIndexed = false;
	                break;
	            }
	            
	            // Check if record is long enough and if the value matches
	            if (record.length <= recordColIdx || !record[recordColIdx].equals(expectedValue)) {
	                matchesAllNonIndexed = false;
	                break;
	            }
	        }
	        if (matchesAllNonIndexed) {
	            finalFilteredResults.add(record);
	        }
	    }
	    return finalFilteredResults;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	// Consider making this private if only used by selectIndex
	private static ArrayList<String []> getallrecords (String tablename){
		Table t = FileManager.loadTable(tablename);
		if (t == null) {
		    System.err.println("Table " + tablename + " not found in getallrecords.");
		    return new ArrayList<>(); // Return empty list if table doesn't exist
		}

		ArrayList<String []> allRecordsList = new ArrayList<>();
		int pageCount = t.getPageCount();
		for(int i = 0; i < pageCount; i++){
			Page currentPage = FileManager.loadTablePage(tablename, i);
			if (currentPage != null) {
			    ArrayList<String []> recordsInPage = currentPage.getRecords();
			    if (recordsInPage != null) {
			        allRecordsList.addAll(recordsInPage);
			    }
			}
		}
		return allRecordsList;
	}
	
	
	
	
	
	
	// Consider making this private
	private static int[] bitmapToIntArray(String bitmapString) {
	    if (bitmapString == null) return new int[0];
	    int[] resultArray = new int[bitmapString.length()];
	    char[] chars = bitmapString.toCharArray();
	    for (int i = 0; i < chars.length; i++) {
	        resultArray[i] = (chars[i] == '1' ? 1 : 0);
	    }
	    return resultArray;
	}
	
	
	
	
	// Consider making this private
	public static int[] bitwiseAnd(int[] bitmapArray1, int[] bitmapArray2) {
	    if (bitmapArray1.length != bitmapArray2.length) {
	        // This case should ideally be handled by the caller or by design
	        // (e.g., all bitmaps for a table have same length)
	        // Throwing an error or returning null/empty might be appropriate.
	        // For now, assume lengths are compatible as per original logic.
	        System.err.println("Error: Bitmaps for AND operation have different lengths.");
	        return new int[Math.min(bitmapArray1.length, bitmapArray2.length)]; // Or throw exception
	    }
	    int[] resultBitmap = new int[bitmapArray1.length];
	    for (int i = 0; i < bitmapArray1.length; i++) {
	        resultBitmap[i] = bitmapArray1[i] & bitmapArray2[i];  
	    }
	    return resultBitmap;
	}
	
	

	
	public static void main(String []args) throws IOException 
	{ 
	FileManager.reset(); 
	String[] cols = {"id","name","major","semester","gpa"}; 
	createTable("student", cols); 
	String[] r1 = {"1", "stud1", "CS", "5", "0.9"}; 
	insert("student", r1); 
	 
	 
	   
	  String[] r2 = {"2", "stud2", "BI", "7", "1.2"}; 
	  insert("student", r2); 
	   
	  String[] r3 = {"3", "stud3", "CS", "2", "2.4"}; 
	  insert("student", r3); 
	   
	  createBitMapIndex("student", "gpa"); 
	  createBitMapIndex("student", "major"); 
	   
	  System.out.println("Bitmap of the value of CS from the major index: "+getValueBits("student", "major", "CS")); 
	  System.out.println("Bitmap of the value of 1.2 from the gpa index: "+getValueBits("student", "gpa", "1.2")); 
	   
	   
	  String[] r4 = {"4", "stud4", "CS", "9", "1.2"}; 
	  insert("student", r4); 
	   
	  String[] r5 = {"5", "stud5", "BI", "4", "3.5"}; 
	  insert("student", r5); 
	   
	  System.out.println("After new insertions:");  
	  System.out.println("Bitmap of the value of CS from the major index: "+getValueBits("student", "major", "CS")); 
	  System.out.println("Bitmap of the value of 1.2 from the gpa index: "+getValueBits("student", "gpa", "1.2")); 
	     
	   
	  System.out.println("Output of selection using index when all columns of the select conditions are indexed:"); 
	  ArrayList<String[]> result1 = selectIndex("student", new String[] 
	{"major","gpa"}, new String[] {"CS","1.2"}); 
	        for (String[] array : result1) { 
	            for (String str : array) { 
	                System.out.print(str + " "); 
	            } 
	            System.out.println(); 
	        } 
	  System.out.println("Last trace of the table: "+getLastTrace("student")); 
	        System.out.println("--------------------------------"); 
	         
	  System.out.println("Output of selection using index when only one column of the columns of the select conditions are indexed:"); 
	  ArrayList<String[]> result2 = selectIndex("student", new String[] 
	{"major","semester"}, new String[] {"CS","5"}); 
	        for (String[] array : result2) { 
	            for (String str : array) { 
	                System.out.print(str + " "); 
	            } 
	            System.out.println(); 
	        } 
	  System.out.println("Last trace of the table: "+getLastTrace("student")); 
	        System.out.println("--------------------------------"); 
	         
	System.out.println("Output of selection using index when some of the columns of the select conditions are indexed:"); 
	ArrayList<String[]> result3 = selectIndex("student", new String[] 
	{"major","semester","gpa" }, new String[] {"CS","5", "0.9"}); 
	for (String[] array : result3) { 
	for (String str : array) { 
	System.out.print(str + " "); 
	} 
	System.out.println(); 
	} 
	System.out.println("Last trace of the table: "+getLastTrace("student")); 
	System.out.println("--------------------------------"); 
	System.out.println("Full Trace of the table:"); 
	System.out.println(getFullTrace("student")); 
	System.out.println("--------------------------------"); 
	System.out.println("The trace of the Tables Folder:"); 
	System.out.println(FileManager.trace()); 
	} 
	   
} 



	
	


