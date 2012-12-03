import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class MeadScan {
	
	public static float averageDeltas(MeadRows meadrows) {
		long total = 0;
		int count = 0;
		for(MeadRow row: meadrows.rows) {
			// Safeguard for bad data
			if (row.getDelta() < 50) {
				total += row.getDelta();
				count++;
			}
		}
		return (total / count);
	}
	
	public static void main(String[] args) throws IOException, NoSuchAlgorithmException {		
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "distillery");
        HTable meadcam = new HTable(config, "meadcam");

        if (args[0].equals("delta")) {
	        Scan s = new Scan();
	        s.addFamily(MeadRow.c_meta);
	        s.setCaching(100);
	        ResultScanner scanner = meadcam.getScanner(s);
	
	        float total = 0f;
	        int count = 0;
	        System.out.println("Scanning...");
	        for (Result r = scanner.next(); r != null; r = scanner.next()) {
	        	MeadRow row = new MeadRow(r);
	        	total += row.getDelta();
	        	count += 1;
	        }
	        System.out.println("Average Delta: " + (total / count));
	        System.out.println(count + " Rows");
        }
        
        if (args[0].equals("get")) {
        	MeadRow row = new MeadRow(meadcam, args[1]);
        	System.out.println("==== " + row.getBrewId() + " ====");
        	System.out.println("Date:\t " + row.getDate());
        	System.out.println("Delta:\t " + row.getDelta());
        	System.out.println("RMS:\t " + row.getRMS());
        }
        
        if (args[0].equals("latest")) {
        	System.out.println("Getting latest...");
        	String brewId = args[1];
        	long minutes = Long.parseLong(args[2]);
        	MeadRows meadrows = new MeadRows(meadcam, brewId, minutes);
        	System.out.println("Count: " + meadrows.rows.size());
        	System.out.println("Average: " + Float.toString(averageDeltas(meadrows)));
        }
        
        meadcam.close();
	}
}
