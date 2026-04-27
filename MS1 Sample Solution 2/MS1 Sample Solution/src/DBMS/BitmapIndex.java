package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BitmapIndex implements Serializable {
	// Changed field name
    private Map<String, ArrayList<Integer>> bitmapStorage;

    public BitmapIndex() {
        this.bitmapStorage = new HashMap<>();
    }


    
    public Map<String, ArrayList<Integer>> getIndex() {
        return this.bitmapStorage;
    }

    public void add(String valueKey, int bitToAdd) {
        ArrayList<Integer> currentBitmap = this.bitmapStorage.get(valueKey);
        if (currentBitmap == null) {
            currentBitmap = new ArrayList<>();
            this.bitmapStorage.put(valueKey, currentBitmap);
        }
        currentBitmap.add(bitToAdd);
    }
    
    
    public ArrayList<String> getAllValues() {
        Set<String> keySet = this.bitmapStorage.keySet();
        return new ArrayList<>(keySet);
    }


    public boolean contains(String keyToSearch) {
        return this.bitmapStorage.containsKey(keyToSearch);
    }

    public ArrayList<Integer> getBitmapForKey(String keyForLookup) {
        return this.bitmapStorage.getOrDefault(keyForLookup, new ArrayList<>());
    }


    public void putBitmap(String targetKey, ArrayList<Integer> bitmapToStore) {
        this.bitmapStorage.put(targetKey, bitmapToStore);
    }
    public void createNew(String newEntryKey, int initialSize) {

        ArrayList<Integer> freshBitmap = new ArrayList<>(initialSize); // Pre-allocate capacity
        for (int i = 0; i < initialSize; i++) {
            freshBitmap.add(Integer.valueOf(0)); // Explicitly using Integer.valueOf
        }
        this.bitmapStorage.put(newEntryKey, freshBitmap);
    }
    
    
    
    
    private ArrayList<Integer> removeBitmapEntry(String keyToRemove) {
        if (keyToRemove == null) {
            // Or throw IllegalArgumentException
            return null;
        }
        return this.bitmapStorage.remove(keyToRemove);
    }
    private boolean isValidBitmap(ArrayList<Integer> bitmap) {
        // This is a stub implementation.
        // A real implementation might check for non-null, non-empty,
        // or specific bit values (e.g., only 0s and 1s).
        return bitmap != null && !bitmap.isEmpty();
    }
    
    
    
    
    
    
    
    
    
    


}
