package freesqlbase;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.sql.Connection;
import java.sql.DriverManager;

public class FreeSQLBase {
	static PipedOutputStream pos;
	static PipedInputStream pis;

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
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println(line);
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
				Connection con = null; // 定义一个MYSQL链接对象
				Class.forName("com.mysql.jdbc.Driver").newInstance(); // MYSQL驱动
				con = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/freebase", "root", "thisissql"); // 链接本地MYSQL
				System.out.print("yes");
			} catch (Exception e) {
				System.out.println("MYSQL ERROR:" + e.getMessage());
			}
			pos = new PipedOutputStream();
			pis = new PipedInputStream(pos);
			FileInputStream s = new FileInputStream(new File("/media/NEWSMY/freebase.gz"));
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
