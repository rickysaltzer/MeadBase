import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.IllegalArgumentException;
import java.security.*;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.PrefixFilter;

public class MeadRows {
	private int caching = 100;
	private final byte[] startKey;
	private final byte[] endKey;
	private final ResultScanner scanner;
	
	private String getMD5Hash(String text) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		MessageDigest md = MessageDigest.getInstance("MD5");
		byte[] hashedBrewId = md.digest(text.getBytes("UTF-8"));
		return new String(Hex.encodeHex(hashedBrewId));
	}
	
	public MeadRows(HTable table) throws IOException {
		startKey = null;
		endKey = null;
		Scan s = new Scan();
		s.addFamily(MeadRow.c_meta);
		s.setCaching(caching);
		this.scanner = table.getScanner(s);
	}
	
	public MeadRows(HTable table, String brewId) throws IOException, NoSuchAlgorithmException {
		startKey = null;
		endKey = null;
		PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes(getMD5Hash(brewId)));
		Scan s = new Scan();
		s.setFilter(prefixFilter);
		s.addFamily(MeadRow.c_meta);
		s.setCaching(caching);
		this.scanner = table.getScanner(s);
	}
		
	public MeadRows(HTable table, String brewId, long minutes) throws IOException, NoSuchAlgorithmException {
		String hashedId = getMD5Hash(brewId);
		startKey = Bytes.toBytes(hashedId + "|" + Long.toString(System.currentTimeMillis()/1000 - (minutes * 60)));
		endKey = Bytes.toBytes(hashedId+ "|" + Long.toString(System.currentTimeMillis()/1000));
		
		Scan s = new Scan();
        s.addFamily(MeadRow.c_meta);
        s.setStartRow(startKey);
        s.setStopRow(endKey);
        s.setCaching(caching);
        this.scanner = table.getScanner(s);
	}
	
	public int getCaching() {
		return this.caching;
	}
	
	public void setCaching(int val) {
		if (val >= 0) {
			this.caching = val;
		}
		else {
			throw new IllegalArgumentException();
		}
	}
	
	public ResultScanner getScanner() {
		return this.scanner;
	}
	
	public String getStartKey() {
		return Bytes.toString(startKey);
	}
	
	public String getStopKey() {
		return Bytes.toString(endKey);
	}
}
