import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Scanner;

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
public class Prog2C {
    public static void main(String[] args){
        Scanner scanner;

        File indexFileRef, dbFileRef;
        RandomAccessFile dbDataStream, indexDataStream;
        String narr1;
        byte[] datebuff = new byte[10];
        String date;
        int caseNum, index, recordLength, input;
        long fileLength;
        Bucket bucket;
        DataRecord rec = new DataRecord();
        byte[] narr1Buff = new byte[rec.getNarr1Len()];

        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

        Prog2B prog2B = new Prog2B();

        dbFileRef = new File(args[0]);
        indexFileRef = new File(args[1]);

        try {
            dbDataStream = new RandomAccessFile(dbFileRef, "rw");
            indexDataStream = new RandomAccessFile(indexFileRef, "rw");

            scanner = new Scanner(System.in);

            while (true) {
                System.out.println("Please enter one or more CPSC case numbers to search for");

                input = scanner.nextInt();

                try {
                    caseNum = input;

                    if(caseNum < 0){
                        return;
                    }
                    bucket = prog2B.readBucket(indexDataStream, caseNum);

                    prog2B.printAll(indexDataStream);
                    index = bucket.search(caseNum);
                    if(index == -1){
                        System.out.println("Key " + caseNum + " not found!");
                    } else {
                        fileLength = dbDataStream.length();
                        dbDataStream.seek(fileLength - 20);
                        rec.fetchLengths(dbDataStream);
                        recordLength = 66 + rec.getStratumLen() + rec.getNarr1Len() + rec.getRaceOtherLen() + rec.getDiagOtherLen() +
                                rec.getNarr2Len();
                        dbDataStream.seek(recordLength * (index - 1));
                        caseNum = dbDataStream.readInt();
                        dbDataStream.readFully(datebuff);
                        date = (new String(datebuff));
                        try {
                            sdf.parse(date);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        dbDataStream.seek(dbDataStream.getFilePointer() + 52 + rec.getDiagOtherLen() + rec.getRaceOtherLen()
                            + rec.getStratumLen());
                        dbDataStream.readFully(narr1Buff);
                        narr1 = new String(narr1Buff);
                        String format = "%-10d%-11s%-" + rec.getNarr1Len() + "s\n";
                        System.out.printf(format, caseNum, date, narr1);
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Invalid format!");
                }
            }
        }
        catch (IOException e ){
            e.printStackTrace();
        }
    }
}
