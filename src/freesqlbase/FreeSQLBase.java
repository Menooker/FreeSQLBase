package freesqlbase;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class FreeSQLBase {
	static PipedOutputStream pos;
	static PipedInputStream pis;
	static Connection con = null; // 定义一个MYSQL链接对象
	static ExecutorService pool = Executors.newFixedThreadPool(4);
	
	static void parse(String[] line)
	{
		System.out.printf("%s,%s,%s\n",line[0],line[1],line[2]);
	}
	
	static private final class StringTask implements Callable<String> {
		public String[] lines;
		public StringTask(String[] p)
		{
			lines=p;
		}
		public String call() {
			// Long operations
			parse(lines);
			return null;
		}
	}
	
	static Thread readth = new Thread() {
		@Override
		public void run() {
			BufferedReader reader = new BufferedReader(new InputStreamReader(pis));
			int i = 0;
			for (;;) {
				i++;
				if (i > 100)
					break;
				String line = null;
				try {
					line = reader.readLine();
					String[] sp=line.split("\t");
					StringTask tsk=new StringTask(sp);
					pool.submit(tsk);
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			try {
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	};

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			try {
				
				Class.forName("com.mysql.jdbc.Driver").newInstance(); // MYSQL驱动
				con = DriverManager.getConnection("jdbc:mysql://202.120.37.25:23334/master", "root", "thisismysql"); // 链接本地MYSQL
				System.out.println("yes");
				/*Statement stmt;
				stmt = con.createStatement();
				stmt.executeUpdate("INSERT INTO test VALUES (1,'KK')");
				ResultSet res = stmt.executeQuery("select * from test");
				int ret_id;
				String name;
				if (res.next()) {
					ret_id = res.getInt(1);
					name = res.getString(2);
					System.out.println(ret_id+" "+name);
				}*/
			} catch (Exception e) {
				System.out.println("MYSQL ERROR:" + e.getMessage());
			} 

			pos = new PipedOutputStream(); pis = new PipedInputStream(pos);
			FileInputStream s = new FileInputStream(
					new File("/home/freebase.gz")); 
			readth.start();
			decompress(s);
			 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void decompress(InputStream is) throws Exception {

		GZIPInputStream gis = new GZIPInputStream(is);
		int count;
		byte data[] = new byte[4096];

		System.out.println("KKK");
		while ((count = gis.read(data, 0, 4096)) != -1) {
			pos.write(data, 0, count);
		}
		pos.write("\n".getBytes());
		gis.close();
	}
}
