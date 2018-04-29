package part1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;

public class ValueOffset implements Writable {
	private String filename;
	private long fileSize;
	private ArrayList<IntWritable> offsetList;
	
	public ValueOffset(){}

	public ValueOffset(String filename) {
		this.filename = filename;
		offsetList = new ArrayList<IntWritable>();
	} 
	
	public void addOffset(IntWritable offset) {
		offsetList.add(offset);
	}
	
	public IntWritable getOffset(int i) {
		return offsetList.get(i);
	}
	
	public String returnFilename(){
		return this.filename;
	}

	public int getTermFreq() {
		return offsetList.size();
	}
	
	public String termFreqLast() {
		double A = (double)offsetList.size();
		double B = (double)fileSize;
		return String.valueOf(A/B);
	}

	public boolean equals(ValueOffset v) {
		return v.returnFilename().equals(filename);
	}
	
	public void recvFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public void sort() {
		Collections.sort(offsetList, new OffsetComparator());
	}
    
    public class OffsetComparator implements Comparator {
        public int compare(Object A, Object B) {
            int AWritable = ((IntWritable)A).get();
            int BWritable = ((IntWritable)B).get();
            return AWritable - BWritable;
        }
    }
	
	@Override
	public void readFields(DataInput in) throws IOException {
		filename = in.readUTF();
		int size = in.readInt();
		this.offsetList = new ArrayList<IntWritable>(size);
		for(int i = 0; i < size; i++) {
			IntWritable termFreq = new IntWritable();
			termFreq.readFields(in);
			this.offsetList.add(termFreq);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(filename);
		out.writeInt(offsetList.size());
		for(IntWritable o: offsetList) {
			o.write(out);
		}
	}
}
