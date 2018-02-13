import java.util.ArrayList;
import java.util.List;

/**
 * The bucket class is used to store the values being inserted in the index file buckets as well as provide supporting
 * methods for the operations that will be performed on the buckets. The maxSize variable will hold the maximum size
 * value for a bucket.
 */
public class Bucket {
    int maxSize, realSize;
    long bucketLength;

    List<BucketEntry> entries; // ArrayList that will store the bucket entries

    /**
     * Constructor to initialize the bucket and set the maxSize variable
     */
    public Bucket(int maxSize) {
        entries = new ArrayList<>();
        this.maxSize = maxSize;
        this.bucketLength = 8 * maxSize; // Bucket length in bytes (2 ints)
    }

    public long getBucketLength(){
        return bucketLength;
    }

    public int getRealSize(){
        return realSize;
    }

    /**
     * search() - find the key in the bucket
     *
     * @param key - the CPSC case number to be searched for
     * @return Index of the entry in the database file or -1 if the case number is not found in the bucket
     */
    public int search(int key) {
        for (BucketEntry entry : entries) {
            if (entry.key == key) return entry.value;
        }
        return -1;
    }

    /**
     * insert() - Put the key into the bucket if there is room.
     *
     * @param key - CPSC case number
     * @param value - Index of the entry in the database file
     * @return Returns true if the key was successfully inserted. Returns false if the bucket is full
     */
    public void insert(int key, int value) {
        if(key == -1 && value == -1){
            entries.add(new BucketEntry(-1, -1));
            return;
        }

        for(int i = 0; i < entries.size(); i++){
            if(entries.get(i).getKey() == -1) {
                entries.get(i).setKey(key);
                entries.get(i).setValue(value);
                realSize++;
                return;
            }
        }
    }

    /**
     *
     * @param i
     * @return
     */
    public BucketEntry get(int i){
        return entries.get(i);
    }
}

/**
 * The purpose of this class is to hold the values that will be stored in the buckets in one object
 *
 * key - the CPSC Case Number
 * value - the index of the case in the database file
 */
class BucketEntry {
    int key; // Case number
    int value; // Database file index value

    /**
     * Constructor to set the entry variables
     * @param key
     * @param value
     */
    public BucketEntry(int key, int value) {
        this.key = key;
        this.value = value;
    }

    public int getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public void setValue(int value) {
        this.value = value;
    }
}