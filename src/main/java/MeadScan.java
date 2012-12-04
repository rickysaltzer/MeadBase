import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

public class MeadScan {
	
	public static float averageDeltas(MeadRows meadrows) {
		float total = 0f;
		int count = 0;
		ResultScanner scanner = meadrows.getScanner();
		try {
			for(Result r: meadrows.getScanner()) {
				MeadRow row = new MeadRow(r);
				// Safeguard for bad data
				total += row.getDelta();
				count++;
			}
		} finally {
			scanner.close();
		}
		
		try {
			System.out.println(count);
			return (total / count);
		} catch (java.lang.ArithmeticException e) {
			return 0;
		}
	}
	
	public static void main(String[] args) throws IOException, NoSuchAlgorithmException {		
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "distillery");
        HTable meadcam = new HTable(config, "meadcam");

        if (args[0].equals("delta")) {
	        MeadRows meadrows = new MeadRows(meadcam);
	        System.out.println("Average Delta: " + averageDeltas(meadrows));
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
        	//long minutes = Long.parseLong(args[2]);
        	MeadRows meadrows = new MeadRows(meadcam, brewId);
        	System.out.println("Average: " + Float.toString(averageDeltas(meadrows)));
        }
        
        meadcam.close();
	}
}
