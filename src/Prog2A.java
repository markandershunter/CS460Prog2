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
import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.List;

class DataRecord{

	private final int DATE_LENGTH = 10;

	// Data fields
    private int caseNum;        // 4 bytes
    private Date date;          // 10 bytes
    private int psu;            // 4 bytes
    private double weight;      // 8 bytes
    private String stratum;     // ?? bytes
    private int age;            // 4 bytes
    private int sex;            // 4 bytes
    private int race;           // 4 bytes
    private String race_other;  // ?? bytes
    private int diag;           // 4 bytes
    private String diag_other;  // ?? bytes
    private int body_part;      // 4 bytes
    private int disposition;    // 4 bytes
    private int location;       // 4 bytes
    private int fmv;            // 4 bytes
    private int prod1;          // 4 bytes
    private int prod2;          // 4 bytes
    private String narr1;       // ?? bytes
    private String narr2;       // ?? bytes
                                // ----------
                                // 66 + stratumLen + raceOtherLen + diagOtherLen + narr1Len + narr2Len

    // Length variables
    private int stratumLen = 0;
    private int raceOtherLen = 0;
    private int diagOtherLen = 0;
    private int narr1Len = 0;
    private int narr2Len = 0;

    // Getters
    public int getCaseNum(){ return(caseNum); }
    public Date getDate(){ return(date); }
    public int getPsu(){ return(psu); }
    public double getWeight(){ return(weight); }
    public String getStratum(){ return(stratum); }
    public int getAge(){ return(age); }
    public int getSex(){ return(sex); }
    public int getRace(){ return(race); }
    public String getRaceOther(){ return(race_other); }
    public int getDiag(){ return(diag); }
    public String getDiagOther(){ return(diag_other); }
    public int getBodyPart(){ return(body_part); }
    public int getDisposition(){ return(disposition); }
    public int getLocation(){ return(location); }
    public int getFmv(){ return(fmv); }
    public int getProd1(){ return(prod1); }
    public int getProd2(){ return(prod2); }
    public String getNarr1(){ return(narr1); }
    public String getNarr2(){ return(narr2); }
    public int getNarr2Len() { return narr2Len;	}
	public int getNarr1Len() { return narr1Len;	}
	public int getDiagOtherLen() { return diagOtherLen;	}
	public int getRaceOtherLen() { return raceOtherLen;	}
	public int getStratumLen() { return stratumLen;	}

    // Setters
    public void setCaseNum(int newNum){ caseNum = newNum; }
    public void setPsu(int newPsu){ psu = newPsu; }
    public void setWeight(double newWeight){ weight = newWeight; }
    public void setStratum(String newStratum){ stratum = newStratum; }
    public void setAge(int newAge){ age = newAge; }
    public void setSex(int newSex){ sex = newSex; }
    public void setRace(int newRace){ race = newRace; }
    public void setRaceOther(String newRaceOther){ race_other = newRaceOther; }
    public void setDiag(int newDiag){ diag = newDiag; }
    public void setDiagOther(String newDiagOther){ diag_other = newDiagOther; }
    public void setBodyPart(int newBodyPart){ body_part = newBodyPart; }
    public void setDisposition(int newDisp){ disposition = newDisp; }
    public void setLocation(int newLocation){ location = newLocation; }
    public void setFmv(int newFmv){ fmv = newFmv; }
    public void setProd1(int newProd1){ prod1 = newProd1; }
    public void setProd2(int newProd2){ prod2 = newProd2; }
    public void setNarr1(String newNarr1){ narr1 = newNarr1; }
    public void setNarr2(String newNarr2){ narr2 = newNarr2; }
	public void setNarr2Len(int narr2Len) {	this.narr2Len = narr2Len; }
	public void setNarr1Len(int narr1Len) {	this.narr1Len = narr1Len; }
	public void setDiagOtherLen(int diagOtherLen) {	this.diagOtherLen = diagOtherLen; }
	public void setRaceOtherLen(int raceOtherLen) {	this.raceOtherLen = raceOtherLen; }
	public void setStratumLen(int stratumLen) {	this.stratumLen = stratumLen; }

	// Parses the string newDate so the parsing algorithm can be reused
    public void setDate(String newDate){
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
            date = sdf.parse(newDate);
        } catch (ParseException e) {
            System.out.format("Date field %s for case number %d is in invalid format!\n", newDate, caseNum);
            System.exit(-1);
        }
    }

    // Dump the DataRecord object into the binary file via the RAF stream
    public void dumpObject(RandomAccessFile stream){

        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

    	// Initialize all buffers for string data in the record
    	StringBuffer stratumBuff = new StringBuffer(stratum);
    	StringBuffer raceOtherBuff = new StringBuffer(race_other);
    	StringBuffer diagOtherBuff = new StringBuffer(diag_other);
    	StringBuffer narr1Buff = new StringBuffer(narr1);
    	StringBuffer narr2Buff = new StringBuffer(narr2);
    	StringBuffer dateBuff = new StringBuffer(sdf.format(date));
    	
    	// Dump data fields to the binary file
    	try{
    		stream.writeInt(caseNum);
    		
    		dateBuff.setLength(DATE_LENGTH);
    		stream.writeBytes(dateBuff.toString());
    		
    		stream.writeInt(psu);
    		stream.writeDouble(weight);
    		
    		stratumBuff.setLength(stratumLen);
    		stream.writeBytes(stratumBuff.toString());
    		
    		stream.writeInt(age);
    		stream.writeInt(sex);
    		stream.writeInt(race);
    		
    		raceOtherBuff.setLength(raceOtherLen);
    		stream.writeBytes(raceOtherBuff.toString());
    		
    		stream.writeInt(diag);
    		
    		diagOtherBuff.setLength(diagOtherLen);
    		stream.writeBytes(diagOtherBuff.toString());
    		
    		stream.writeInt(body_part);
    		stream.writeInt(disposition);
    		stream.writeInt(location);
    		stream.writeInt(fmv);
    		stream.writeInt(prod1);
    		stream.writeInt(prod2);
    		
    		narr1Buff.setLength(narr1Len);
    		stream.writeBytes(narr1Buff.toString());
    		
    		narr2Buff.setLength(narr2Len);
    		stream.writeBytes(narr2Buff.toString());
    		
    	} catch(IOException e){
    		System.out.println("I/O ERROR: Couldn't write to the file;\n\t"
                    + "perhaps the file system is full?");
    		System.exit(-1);
    	}
    }

    // Dump the final max lengths at the end of the binary file
    public void dumpLengths(RandomAccessFile stream){
        try {
            stream.writeInt(stratumLen);
            stream.writeInt(raceOtherLen);
            stream.writeInt(diagOtherLen);
            stream.writeInt(narr1Len);
            stream.writeInt(narr2Len);
        } catch (IOException e) {
            System.out.println("I/O ERROR: Couldn't write to the file;\n\t"
                    + "perhaps the file system is full?");
            System.exit(-1);
        }
    }

    // Fetch a data record from the binary file
    public void fetchObject(RandomAccessFile stream){

    	byte[] stratumBuff = new byte[stratumLen];
    	byte[] dateBuff = new byte[DATE_LENGTH];
    	byte[] raceOtherBuff = new byte[raceOtherLen];
    	byte[] diagOtherBuff = new byte[diagOtherLen];
    	byte[] narr1Buff = new byte[narr1Len];
    	byte[] narr2Buff = new byte[narr2Len];

    	try{

    		caseNum = stream.readInt();

    		stream.readFully(dateBuff);
    		setDate(new String(dateBuff));



            psu = stream.readInt();
            weight = stream.readDouble();

            stream.readFully(stratumBuff);
            stratum = new String(stratumBuff);

            age = stream.readInt();
            sex = stream.readInt();
            race = stream.readInt();

            stream.readFully(raceOtherBuff);
            race_other = new String(raceOtherBuff);

            diag = stream.readInt();

            stream.readFully(diagOtherBuff);
            diag_other = new String(diagOtherBuff);

            body_part = stream.readInt();
            disposition = stream.readInt();
            location = stream.readInt();
            fmv = stream.readInt();
            prod1 = stream.readInt();
            prod2 = stream.readInt();

            stream.readFully(narr1Buff);
            narr1 = new String(narr1Buff);

            stream.readFully(narr2Buff);
            narr2 = new String(narr2Buff);

        } catch(IOException e){
    		System.out.println("I/O ERROR: Couldn't read from the file;\n\t"
                    + "is the file accessible?");
    		System.exit(-1);
    	}
    }

    // Fetch the final max lengths from the end of the binary file
    public void fetchLengths(RandomAccessFile stream){
        try {
            stratumLen = stream.readInt();
            raceOtherLen = stream.readInt();
            diagOtherLen = stream.readInt();
            narr1Len = stream.readInt();
            narr2Len = stream.readInt();
        } catch (IOException e) {
            System.out.println("I/O ERROR: Couldn't read from the file;\n\t"
                    + "is the file accessible?");
            System.exit(-1);
        }

    }

}

class DataRecordComparator implements Comparator<DataRecord> {
    @Override
    public int compare(DataRecord rec1, DataRecord rec2) {
        return rec1.getCaseNum() - rec2.getCaseNum();
    }
}

public class Prog2A {
    public static void main(String[] args){

        final int DEBUGFLAG = 0;

        File file, fileRef;
        String filepath;
        String line;
        int linesToRead;
        int lineCount = 0;
        RandomAccessFile dataStream = null;
        DataRecord rec1;
        long numberOfRecords = 0;
        String[] fields;
        List<DataRecord> dataRecords = new ArrayList<>();

        int stratumLen = 0;
        int raceOtherLen = 0;
        int diagOtherLen = 0;
        int narr1Len = 0;
        int narr2Len = 0;

        // Filepath of the user-specified input file
        filepath = args[0];

        // Set the linesToRead specified by the user. All lines are read if the command line argument is left blank
        if(args.length > 1)
            linesToRead = Integer.parseInt(args[1]);
        else
            linesToRead = 1000000;

        // Initialize user-specified file
        file = new File(filepath);

        // Initialize binary file
        fileRef = new File(filepath.substring(filepath.lastIndexOf("/") + 1, filepath.lastIndexOf("."))+ ".bin");

        if(fileRef.exists()){
            fileRef.delete();
        }

        // Initialize RAF
        try {
            dataStream = new RandomAccessFile(fileRef,"rw");
        } catch (IOException e) {
            System.out.println("I/O ERROR: Something went wrong with the "
                    + "creation of the RandomAccessFile object.");
            System.exit(-1);
        }

        // Time to parse the input file specified by the user
        try {

            // Initialize file reader
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            // Loop through the file until all lines are parsed or the user-specified
            // line count is reached
            while(lineCount < linesToRead && (line = bufferedReader.readLine()) != null){
                if(lineCount != 0) {

                    rec1 = new DataRecord();

                    // Split all lines after the columns name by tab separators
                    fields = line.split("[\t]");

                    try {
                        // Assign to the corresponding data values
                        rec1.setCaseNum(Integer.parseInt(fields[0]));
                        rec1.setDate(fields[1]);
                        rec1.setPsu(Integer.parseInt(fields[2]));
                        rec1.setWeight(Double.parseDouble(fields[3]));
                        rec1.setStratum(fields[4]);
                        rec1.setAge(Integer.parseInt(fields[5]));
                        rec1.setSex(Integer.parseInt(fields[6]));
                        rec1.setRace(Integer.parseInt(fields[7]));
                        rec1.setRaceOther(fields[8]);
                        rec1.setDiag(Integer.parseInt(fields[9]));
                        rec1.setDiagOther(fields[10]);
                        rec1.setBodyPart(Integer.parseInt(fields[11]));
                        rec1.setDisposition(Integer.parseInt(fields[12]));
                        rec1.setLocation(Integer.parseInt(fields[13]));
                        rec1.setFmv(Integer.parseInt(fields[14]));
                        rec1.setProd1(Integer.parseInt(fields[15]));

                        // There are cases where the prod2 field is empty. Per the spec,
                        // that case is handled by setting the value to -1.
                        try {
                            rec1.setProd2(Integer.parseInt(fields[16]));
                        } catch(NumberFormatException e){
                            rec1.setProd2(-1);
                        }

                        rec1.setNarr1(fields[17]);

                        // In some cases, the last value, narr2, is blank. Since the split
                        // function will automatically trim this, we have to manually add
                        // an empty string to avoid an IndexOutOfBoundsException
                        if(fields.length < 19)
                            rec1.setNarr2("");
                        else
                            rec1.setNarr2(fields[18]);

                    } catch(IndexOutOfBoundsException e){
                        System.out.format("Error! Line %d is incomplete\n", lineCount);
                        System.exit(-1);
                    }

                    // Update the max lengths
                    if(rec1.getStratum().length() > stratumLen)
                        stratumLen = rec1.getStratum().length();

                    if(rec1.getRaceOther().length() > raceOtherLen)
                        raceOtherLen = rec1.getRaceOther().length();

                    if(rec1.getDiagOther().length() > diagOtherLen)
                        diagOtherLen = rec1.getDiagOther().length();

                    if(rec1.getNarr1().length() > narr1Len)
                        narr1Len = rec1.getNarr1().length();

                    if(rec1.getNarr2().length() > narr2Len)
                        narr2Len = rec1.getNarr2().length();

                    // Add the recently parsed DataRecord to the ArrayList
                    dataRecords.add(rec1);
                    numberOfRecords++;
                }

                // Increment count of lines read
                lineCount++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Set the final max lengths in each DataRecord
        for(DataRecord rec: dataRecords){
            rec.setStratumLen(stratumLen);
            rec.setRaceOtherLen(raceOtherLen);
            rec.setDiagOtherLen(diagOtherLen);
            rec.setNarr1Len(narr1Len);
            rec.setNarr2Len(narr2Len);
        }

        // Sort the DataRecord ArrayList in ascending order
        dataRecords.sort(new DataRecordComparator());

        // Tell all DataRecord objects in the ArrayList to write themselves
        for(DataRecord rec: dataRecords){
            rec.dumpObject(dataStream);
        }

        System.out.println("\nThere are " + numberOfRecords + " records in the file.\n");

        if(dataRecords.size() > 0)
            dataRecords.get(0).dumpLengths(dataStream);

        // Clean-up by closing the file
        try {
            dataStream.close();
        } catch (IOException e) {
            System.out.println("VERY STRANGE I/O ERROR: Couldn't close " + "the file!");
        }
    }

}
