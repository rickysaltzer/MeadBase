import java.util.Date;
import java.io.IOException;
import java.lang.ArrayIndexOutOfBoundsException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Get;


public class MeadRow {
	public static final byte[] c_meta = Bytes.toBytes("meta");
	private final float delta;
	private final float rms;
	private String brewId;
	private String date;
	private String key;
	
	public MeadRow(Result row) {
		this.key = Bytes.toString(row.getRow());
		this.delta = Float.parseFloat(Bytes.toString(row.getValue(c_meta, Bytes.toBytes("delta"))));
		this.rms = Float.parseFloat(Bytes.toString(row.getValue(c_meta, Bytes.toBytes("rms"))));
		this.brewId = Bytes.toString(row.getValue(c_meta, Bytes.toBytes("brew_id")));
		this.date = Bytes.toString(row.getValue(c_meta, Bytes.toBytes("time")));
	}
	
	public MeadRow(HTable table, String key) throws IOException {
		Get get = new Get(Bytes.toBytes(key));
		get.addFamily(c_meta);
		Result row = table.get(get);
		this.key = Bytes.toString(row.getRow());
		this.delta = Float.parseFloat(Bytes.toString(row.getValue(c_meta, Bytes.toBytes("delta"))));
		this.rms = Float.parseFloat(Bytes.toString(row.getValue(c_meta, Bytes.toBytes("rms"))));
		this.brewId = Bytes.toString(row.getValue(c_meta, Bytes.toBytes("brew_id")));
		this.date = Bytes.toString(row.getValue(c_meta, Bytes.toBytes("time")));
	}
	
	public boolean save(HTable table) throws IOException {
		Put put = new Put(Bytes.toBytes(getKey()));
		put.add(c_meta, Bytes.toBytes("time"), Bytes.toBytes(this.date));
		put.add(c_meta, Bytes.toBytes("brew_id"), Bytes.toBytes(this.brewId));
		put.add(c_meta, Bytes.toBytes("rms"), Bytes.toBytes(Float.toString(this.rms)));
		put.add(c_meta, Bytes.toBytes("delta"), Bytes.toBytes(Float.toString(this.rms)));
		table.put(put);
		return true;
	}
	
	public float getDelta() {
		return this.delta;
	}
	
	public float getRMS() {
		return this.rms;
	}
	
	public String getKey() {
		return this.key;
	}
	
	public String getBrewId() {
		return this.brewId;
	}
	
	public Date getDate() {
		try {
			return new Date(Long.parseLong(this.date.split("\\.")[0]) * 1000);
		} catch (ArrayIndexOutOfBoundsException e) {
			return null;
		} finally {
			// nothing
		}
	}
	
}