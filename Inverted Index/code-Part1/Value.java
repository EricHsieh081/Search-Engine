package part1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;

public class Value implements Writable {
	ArrayList<ValueOffset> documents;
	
	public Value() {
		this.documents = new ArrayList<ValueOffset>();
	}
	
	public Value(String filename) {
		this.documents = new ArrayList<ValueOffset>();
		documents.add(new ValueOffset(filename));
	}

	public ValueOffset addValueOffset(String filename) {
		ValueOffset v = new ValueOffset(filename);
		documents.add(v);
		return v;
	}	

	public void addOffset(String filename, int offset) {
		for(int i = 0; i < documents.size(); i++) {
			ValueOffset v = documents.get(i);
			if(v.returnFilename().equals(filename)) {
				v.addOffset(new IntWritable(offset));
				break;
			}
		}	
	}

	public void addOffsets(int index, ValueOffset give) {
		ValueOffset get = documents.get(index);
		for(int i = 0; i < give.getTermFreq(); i++) {
			get.addOffset(give.getOffset(i));
		}
	}

	public void addOffsets(ValueOffset give) {
		ValueOffset get = addValueOffset(give.returnFilename());
		for(int i = 0; i < give.getTermFreq(); i++) {
			get.addOffset(give.getOffset(i));
		}
	}
	
	public int getDocumentSize() {
		return documents.size();
	}

	public ValueOffset getValueOffset(int index) {
		return documents.get(index);
	}

	public void merge(Value v) {
		for(int i = 0; i < v.getDocumentSize(); i++) {
			ValueOffset curOffset = v.getValueOffset(i);
			int j;
			for(j = 0; j < getDocumentSize(); j++) {
				if(getValueOffset(j).equals(curOffset)){
					addOffsets(j, curOffset);
					break;
				}
			}
			if(j == getDocumentSize()) {
				addOffsets(curOffset);
			}
		}
	}
	
	public void sort() {
		Collections.sort(documents, new TermComparator());
		for(int i = 0; i < documents.size(); i++) {
			documents.get(i).sort();
		}
	}	
	
	public class TermComparator implements Comparator {
		public int compare(Object term1, Object term2) {
			String str1 = ((ValueOffset)term1).returnFilename();
			String str2 = ((ValueOffset)term2).returnFilename();
			return str1.compareTo(str2);
		}
	}

	public String outputValue() {
		String result = "";
		for(int i = 0; i < getDocumentSize(); i++) {
			ValueOffset v = documents.get(i);
			result += v.returnFilename() + "#" ;
		}
		return result;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(documents.size());
		for(ValueOffset v : documents) {
			v.write(out);
		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		documents = new ArrayList<ValueOffset>(size);
		for(int i = 0; i < size; i++) {
			ValueOffset v = new ValueOffset();
			v.readFields(in);
			documents.add(v);
		}
	}
}
