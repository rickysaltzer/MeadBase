import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.security.*;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class MeadRows {
	public final List<MeadRow> rows = new ArrayList<MeadRow>();
	private byte[] startKey;
	private byte[] endKey;
	
	public MeadRows(HTable table, String brewId, long minutes) throws NoSuchAlgorithmException, IOException {
		MessageDigest md = MessageDigest.getInstance("MD5");
		byte[] hashedBrewId = md.digest(brewId.getBytes("UTF-8"));
		startKey = Bytes.toBytes(new String(Hex.encodeHex(hashedBrewId)) + "|" + Long.toString(System.currentTimeMillis()/1000 - (minutes * 60)));
		endKey = Bytes.toBytes(new String(Hex.encodeHex(hashedBrewId)) + "|" + Long.toString(System.currentTimeMillis()/1000));
		
		Scan s = new Scan();
        s.addFamily(MeadRow.c_meta);
        s.setStartRow(startKey);
        s.setStopRow(endKey);
        s.setCaching(100);
        ResultScanner scanner = table.getScanner(s);
        for (Result r = scanner.next(); r != null; r = scanner.next()) {
        	rows.add(new MeadRow(r));
        }
	}
	
	public String getStartKey() {
		return Bytes.toString(startKey);
	}
	
	public String getStopKey() {
		return Bytes.toString(endKey);
	}
}
