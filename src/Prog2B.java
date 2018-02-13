import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Name: Mark Hunter
 * Course Name: CS460 - Databases
 * Assignment: Program 2 - Simplified Linear Hashing
 * Instructor: Lester McCann
 * TAs: Jacob Combs and Aakash Rathore
 * Due Date: Thursday, February 8th
 * Description: This program uses an external hashing process to easily store and search for entries from hospital records
 *              The records are stored in order of CPSC Case Number in a binary file. This is done by running Prog2A.java
 *              The binary file is then read record by record. Each record is parsed to acquire the case number. The
 *              case number is hashed and stored in the appropriate bucket in the index file for easy retrieval. If the
 *              bucket that the record must be placed in is full, the number of buckets is doubled and each bucket is
 *              traversed through to rehash the values. This can be achieved by running Prob2B.java. Once the buckets
 *              are full, Prog2C.java is run to prompt the user for a case number to search for. That case number is
 *              hashed and searched for in the appropriate bucket. If it exists, the index value stored with it is used
 *              to read the database file. After reading the database file, the program prints out the case number, the
 *              date of the record, and the narr1 field. The user is then prompted to input another case number. This
 *              process repeats until the user inputs a negative number.
 */

public class Prog2B {
    static final int MAX_SIZE = 50;

    public int H = 0;

    public static void main(String[] args){
        File dbFileRef, indexFileRef;
        RandomAccessFile dbDataStream = null;
        RandomAccessFile indexDataStream = null;
        DataRecord rec = new DataRecord();
        long fileLength;
        int caseNumTemp, recordLength, recordIndex;
        Bucket bucket;

        Prog2B prog2b = new Prog2B();

        dbFileRef = new File(args[0]);
        indexFileRef = new File("simplelinear.idx");

        if(indexFileRef.exists())
            indexFileRef.delete();

        try{
            indexDataStream = new RandomAccessFile(indexFileRef, "rw");
            dbDataStream = new RandomAccessFile(dbFileRef, "rw");
            fileLength = dbDataStream.length();
            dbDataStream.seek(fileLength - 20);
            rec.fetchLengths(dbDataStream);
            recordLength = 66 + rec.getStratumLen() + rec.getNarr1Len() + rec.getRaceOtherLen() + rec.getDiagOtherLen() +
                    rec.getNarr2Len();
            dbDataStream.seek(0);

            recordIndex = 1;

            bucket = prog2b.initBucket();
            prog2b.writeBucket(indexDataStream, 0, bucket);

            bucket = prog2b.initBucket();
            prog2b.writeBucket(indexDataStream, 1, bucket);

            // loop until the pointer reaches the lengths at the end of the file
            while(dbDataStream.getFilePointer() < dbDataStream.length() - 20){
                caseNumTemp = dbDataStream.readInt();
                bucket = prog2b.readBucket(indexDataStream, caseNumTemp);

                if(bucket.getRealSize() == MAX_SIZE){
                    prog2b.splitBuckets(indexDataStream);
                    bucket = prog2b.readBucket(indexDataStream, caseNumTemp);
                }

                bucket.insert(caseNumTemp, recordIndex);

                indexDataStream.seek((long) prog2b.hash(caseNumTemp) * bucket.getBucketLength());

                prog2b.writeBucket(indexDataStream, prog2b.hash(caseNumTemp), bucket);

                // Move the database file pointer to the front of the next record
                dbDataStream.seek(dbDataStream.getFilePointer() + recordLength - 4);
                recordIndex++;
            }

        } catch(IOException e){
            System.out.println("I/O Error: something went wrong when attempting to set the file pointer");
            System.exit(-1);
        }

        prog2b.printSpecs(indexDataStream);

        // Clean-up by closing the file
        try {
            dbDataStream.close();
        } catch (IOException e) {
            System.out.println("VERY STRANGE I/O ERROR: Couldn't close " + "the file!");
        }

        // Clean-up by closing the file
        try {
            indexDataStream.close();
        } catch (IOException e) {
            System.out.println("VERY STRANGE I/O ERROR: Couldn't close " + "the file!");
        }


    }

    private void printSpecs(RandomAccessFile indexDataStream) {
        Bucket[] buckets;
        int bucketCount, key, value;
        int min = MAX_SIZE;
        int max = 0;
        int total = 0;
        int i = 0;
        int j = 0;

        buckets = new Bucket[(int) Math.pow(2, H + 1)];

        for(int z = 0; z < Math.pow(2, H + 1); z++){
            buckets[z] = initBucket();
        }

        // Final Bucket count
        bucketCount = buckets.length;
        System.out.println("Final bucket count: " + bucketCount);

        try{
            indexDataStream.seek(0);
            while(indexDataStream.getFilePointer() < indexDataStream.length()){
                if(j == MAX_SIZE) {
                    j = 0;
                    i++;
                }

                key = indexDataStream.readInt();
                value = indexDataStream.readInt();
                if(key != -1)
                    buckets[i].insert(key, value);
                j++;
            }
        } catch (IOException e){
            e.printStackTrace();
            System.out.println("IO Error: there was an issue when printing out the final specs");
        }



        for (Bucket bucket : buckets){
            if (bucket.getRealSize() < min) {
                min = bucket.getRealSize();
            }
            if (bucket.getRealSize() > max) {
                max = bucket.getRealSize();
            }
            total = total + bucket.getRealSize();
        }
        // Bucket min
        System.out.println("Smallest number of records in one bucket: " + min);

        // Bucket max
        System.out.println("Most records in one bucket: " + max);

        // Bucket mean
        System.out.println("Mean number of records in one bucket: " + (total / buckets.length));
    }

    /**
     *
     * @return
     */
    public Bucket initBucket(){
       Bucket bucket = new Bucket(MAX_SIZE);

       for(int i = 0; i < MAX_SIZE; i++){
           bucket.insert(-1, -1);
       }

       return bucket;
    }

    /**
     *
     * @return
     */
    public int getH(){
        return H;
    }

    /**
     * splitBuckets() - SplitBuckets is used to expand the bucket size and spread out values to make room for new entries. The first step
     * it takes is to initilize H new buckets and then double the H value to represent the new number of buckets. After
     * the new buckets have been created it moves on to the rehashing portion of the process. It loops through each entry
     * in each bucket, rehashes them, then inserts them into the appropriate bucket. After all the values have been hashed
     * and inserted, the updated buckets are written back to the file.
     *
     * return - nothing
     */
    public void splitBuckets(RandomAccessFile indexDataStream) {
        int key, value, bucketNum;

        H++;

        Bucket[] buckets;
        buckets = new Bucket[(int) Math.pow(2, H + 1)];

        for(int i = 0; i < Math.pow(2, H + 1); i++){
            buckets[i] = initBucket();
        }

        try {
            indexDataStream.seek(0);
            while(indexDataStream.getFilePointer() < indexDataStream.length()){
                key = indexDataStream.readInt();
                value = indexDataStream.readInt();
                if(key != -1){
                    bucketNum = hash(key);
                    buckets[bucketNum].insert(key, value);
                }
            }

            for(int i = 0; i < Math.pow(2, H + 1); i++){
                writeBucket(indexDataStream, i, buckets[i]);
            }
        } catch (IOException e){
            System.out.println("IO Error: there was an error when trying to split the buckets");
            System.exit(-1);
        }
    }

    /**
     *
     * @param indexDataStream
     * @param caseNum
     */
    public Bucket readBucket(RandomAccessFile indexDataStream, int caseNum){
        int readCaseNum, readIndexValue;

        int bucketNumber = hash(caseNum);

        Bucket bucket = initBucket();

        try {
            indexDataStream.seek(bucketNumber * bucket.getBucketLength());

            while (indexDataStream.getFilePointer() < (bucketNumber + 1) * bucket.getBucketLength()) {
                readCaseNum = indexDataStream.readInt();
                readIndexValue = indexDataStream.readInt();
                bucket.insert(readCaseNum, readIndexValue);
            }
        } catch (IOException e){
            System.out.println("IO Error: something went wrong when attempting to read the bucket from the index file");
            System.exit(-2);
        }
        return bucket;
    }

    /**
     *
     * @param indexDataStream
     * @param bucketNum
     * @param bucket
     */
    public void writeBucket(RandomAccessFile indexDataStream, int bucketNum, Bucket bucket){
        try {
            indexDataStream.seek((long) bucketNum * bucket.getBucketLength());
            for(int i = 0; i < MAX_SIZE; i++){
                indexDataStream.writeInt(bucket.get(i).getKey());
                indexDataStream.writeInt(bucket.get(i).getValue());
            }
        } catch (IOException e) {
            System.out.println("IO Error: There was an error when trying to write the bucket to the index file");
            System.exit(-1);
        }
    }

    /**
     * hash() - the hash function used to determine which bucket a key will be stored in.
     *
     * @param key - the CPSC case number to be stored
     * @return hashed key value
     */
    public int hash(int key){
        return (int) (key % Math.pow(2, H + 1));
    }

    public void printAll(RandomAccessFile indexDataStream){
        try {
            indexDataStream.seek(0);
            int i = 0;
            int bucketCount = 0;

            System.out.println("---------------Bucket Contents---------------");

            while(indexDataStream.getFilePointer() < indexDataStream.length()){
               if(i == 0) {
                    System.out.println("\nBucket Index " + bucketCount);
               }

               System.out.print(indexDataStream.readInt());
               System.out.println("       " + indexDataStream.readInt());

               if(i == MAX_SIZE - 1) {
                    bucketCount++;
                    i = 0;
               }else
                    i++;
            }
        } catch(IOException e){
            System.out.println("IO Error: there was an error when attempting to print out the contents of the index file");
            System.exit(-1);
        }
    }

    public void setH(int H){
        System.out.println(H);
        this.H = H;
    }
}
