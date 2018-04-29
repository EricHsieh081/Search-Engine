import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.*;
import java.io.*;
import java.net.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class HBaseTest {
	private static Configuration conf = null;

	// You can lookup usage of these api from this website. =)
	// http://hbase.apache.org/0.94/apidocs/

	static {
		conf = HBaseConfiguration.create();
	}

	public static void createTable(String tableName, String[] colFamilys) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			System.out.println("Table already exists!");
		} else {
			// TODO add column families to HTableDescriptor instance by for loop
			HTableDescriptor table = new HTableDescriptor(tableName.getBytes());
			for (String colName : colFamilys) {
				table.addFamily(new HColumnDescriptor(colName));
			}
			// TODO admin creates table by HTableDescriptor instance
			admin.createTable(table);
			System.out.println("Create table " + tableName);
		}
	}

	public static void removeTable(String tableName) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		// TODO disable & drop table
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("Remove table " + tableName);
		} else {
			System.out.println("No such table " + tableName);
		}
	}

	public static void addRecord(String tableName, String rowKey, String colFamily,
			String qualifier, String value) throws Exception {
		HTable table = new HTable(conf, tableName);
		// TODO use Put to wrap information and put it to HTable instance, table.
		Put p = new Put(rowKey.getBytes());
		p.add(colFamily.getBytes(), qualifier.getBytes(), value.getBytes());
		table.put(p);
		table.close();
		System.out.println("Insert record " + rowKey + " to table " + tableName + " ok.");
	}

	public static void deleteRecord(String tableName, String rowKey) throws Exception {
		HTable table = new HTable(conf, tableName);	
		// TODO use Delete to wrap key information and use HTable api to delete it.
		Delete d = new Delete(rowKey.getBytes());
		table.delete(d);
		System.out.println("Delete record " + rowKey + " from table " + tableName + " ok.");
	}

	public static void getRecord(String tableName, String rowKey) throws Exception {
		HTable table = new HTable(conf, tableName);
		
		Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for (KeyValue kv : rs.raw()) {
			System.out.print(new String(kv.getRow()) + " ");	
			System.out.print(new String(kv.getFamily()) + ":");
			System.out.print(new String(kv.getQualifier()) + " "); 
			System.out.print(kv.getTimestamp() + " ");
			System.out.println(new String(kv.getValue()));
		}
	}
	
	public static void getAllRecord(String tableName) throws Exception {
		HTable table = new HTable(conf, tableName);
		Scan s = new Scan();
		ResultScanner rs = table.getScanner(s);
		for (Result r: rs) {
			for (KeyValue kv : r.raw()) {
				System.out.print(new String(kv.getRow()) + " ");
				System.out.print(new String(kv.getFamily()) + ":");
				System.out.print(new String(kv.getQualifier()) + " ");
				System.out.print(kv.getTimestamp() + " ");
				System.out.println(new String(kv.getValue()));
			}
		}
	
	}
	
	public static void loadDataToHbase(String directory) throws Exception {

		// create table
		String tableName = "100062243_" + directory;
		String[] colFamily = {directory};
		String[] qualifiers = {"A", "B"};

		if(directory.equals("pagerank_table")) {
			qualifiers[0] = "title";
			qualifiers[1] = "rank";
		}
		else {
			qualifiers[0] = "word"; 
			qualifiers[1] = "title";
		}
		removeTable(tableName);
		createTable(tableName, colFamily);


		// get file numbers
		int fileNum = 0;
		if(directory.equals("pagerank_table")) {
			fileNum = getFileNum("hdfs://Quanta006:8100/user/100062243/HW3/pagerank_table/") - 1;
		}
		else {
			fileNum = getFileNum("hdfs://Quanta006:8100/user/100062243/HW3/inverted_table/") - 2;
		}

		// get all data from files
		String line;
		String inputFile;
		StringBuffer inputLine = new StringBuffer();
		for(int i = 0; i < fileNum; i++) {
			if(i >= 10) {
				inputFile = "hdfs://Quanta006:8100/user/100062243/HW3/" + directory + "/part-000" + i;
			}
			else {
				inputFile = "hdfs://Quanta006:8100/user/100062243/HW3/" + directory + "/part-0000" + i;
			}
			Path inputFilePath = new Path(inputFile); 
			FileSystem fs = inputFilePath.getFileSystem(new Configuration());
			FSDataInputStream fsdI = fs.open(inputFilePath);
			BufferedReader br = new BufferedReader( new InputStreamReader(fsdI));
		
			while((line = br.readLine()) != null) {
				inputLine.append(line);
				inputLine.append("\n");
			}

			br.close();
			fsdI.close();
		}
		String data;
		data = inputLine.toString();

		// analysis the data and assign value to tables
		if(directory.equals("pagerank_table")) {
			StringTokenizer tokens = new StringTokenizer(data, "\n");
			String curTemp, curTitle, curRank;
			while(tokens.hasMoreTokens()) {
				curTemp = tokens.nextToken();
				String[] temp = curTemp.split(",");
				int i;
				for(i = 1; i < temp.length; i++) {
					if(temp[i].charAt(0) >= '0' && temp[i].charAt(0) <= '9')
						break;
					temp[0] = temp[0] + "," + temp[i];
				}
				curTitle = temp[0];
				curTitle = curTitle.substring(1, curTitle.length());
				curRank = temp[i];
				curRank = curRank.substring(0, curRank.length()-1);
				addRecord(tableName, curTitle, colFamily[0], qualifiers[0], curTitle);
				addRecord(tableName, curTitle, colFamily[0], qualifiers[1], curRank);
			}
		} else {
			StringTokenizer tokens = new StringTokenizer(data, "\n");
			String curTemp, curWord, curTitles;
			while(tokens.hasMoreTokens()) {
				curTemp = tokens.nextToken();
				String[] temp = curTemp.split("\t");
				curWord = temp[0];
				curTitles = temp[1];
				//curTitles = curTitles.substring(0, curTitles.length());
				addRecord(tableName, curWord, colFamily[0], qualifiers[0], curWord);
				addRecord(tableName, curWord, colFamily[0], qualifiers[1], curTitles);
			}
		}
	}

	public static int getFileNum (String p) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		Path path = new Path(p);
		if (!fileSystem.exists(path)) {
			System.out.println("Wrong path");
			return 0;
		}
		FileStatus[] status = fileSystem.listStatus(path);
		return status.length;
	}


	public static void main(String[] args) throws Exception {
    	// TOOD write your code here =)
    	HBaseTest h = new HBaseTest();
    	Scanner scanner = new Scanner(System.in);

		HBaseAdmin admin = new HBaseAdmin(conf);
		//loadDataToHbase("pagerank_table");
		//loadDataToHbase("inverted_table");
		if(admin.tableExists("100062243_pagerank_table") && admin.tableExists("100062243_inverted_table")) {
			System.out.println("The tables exist, want to rebuild? (Y/N)");
			String rebuild = scanner.nextLine();
			if(rebuild.equals("Y")) {
				loadDataToHbase("pagerank_table");
				loadDataToHbase("inverted_table");
			} 
		} else {
			loadDataToHbase("pagerank_table");
			loadDataToHbase("inverted_table");
		}

		HTable inverted_table = new HTable(conf, "100062243_inverted_table");
		HTable pagerank_table = new HTable(conf, "100062243_pagerank_table");

		String order, word = "", title;
		Set<String> titleSet = new HashSet<String>();
		ArrayList<TitleToValue> pageRankList = new ArrayList<TitleToValue>();
		TitleToValue curPageRank;
		while(true) {
			titleSet.clear();
			pageRankList.clear();
			System.out.println("Please type the word(s) you want to search:");
			order = scanner.nextLine();
			StringTokenizer tokens = new StringTokenizer(order, " ");
			while(tokens.hasMoreTokens()) {
				word = tokens.nextToken();
				Get get = new Get(word.getBytes());
        		Result rs = inverted_table.get(get);
        		String titles = new String(rs.getValue("inverted_table".getBytes(), "title".getBytes()), "UTF-8");
        		StringTokenizer titleTokens = new StringTokenizer(titles, "#");
        	
        		while(titleTokens.hasMoreTokens()) {
        			title = titleTokens.nextToken();
        			titleSet.add(title);
        		}
        	}
        	Iterator iter = titleSet.iterator();
        	while (iter.hasNext()) {
        		title = iter.next().toString();
        		//System.out.println("get titles:" + title);
        		int i = 0;
        		/*for(i = 0; i < title.length(); i++) {
        			if(title.charAt(i) != ' ')
        				break;
        		}*/
        		title = title.substring(i, title.length());
        		//System.out.println("title:" + title);
        		Get get = new Get(title.getBytes());
        		Result rs = pagerank_table.get(get);
        		String rank = new String(rs.getValue("pagerank_table".getBytes(), "rank".getBytes()), "UTF-8");
        		//System.out.println("title:" + title + " rank:" + rank);
        		curPageRank = new TitleToValue(title);
        		curPageRank.recvRank(Double.parseDouble(rank));
        		pageRankList.add(curPageRank);
        	}
        	Collections.sort(pageRankList);
        	for(int i = 0; i < pageRankList.size();i++) {
        		System.out.println("Page: " + pageRankList.get(i).title + " || Rank: " + pageRankList.get(i).rank);
        	}
		}
	}
}
