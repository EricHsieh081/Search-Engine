package part1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordCountGroupComparator extends WritableComparator {

	public WordCountGroupComparator() {
		super(Text.class, true);
	}	

	// TODO group by start letter
	public int compare(WritableComparable o1, WritableComparable o2) {
		String key1 = ((Text) o1).toString();
		String key2 = ((Text) o2).toString();
		
		String key11 = key1.toLowerCase();
		String key22 = key2.toLowerCase();
		
		int flag = key1.compareTo(key2);
		int flagI = key11.compareTo(key22);
		if(flag == 0) {
			return 0;
		} else if(flag > 0) {
			if(flagI >= 0)
				return 1;
			else
				return -1;
		} else {
			if(flagI <= 0)
				return -1;
			else
				return 1;
		}
	}
}
