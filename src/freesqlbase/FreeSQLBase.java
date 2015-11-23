package freesqlbase;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class FreeSQLBase {
	static PipedOutputStream pos;
	static PipedInputStream pis;
	static Connection con = null; // 定义一个MYSQL链接对象
	static ExecutorService pool = Executors.newFixedThreadPool(4);
	//static ThreadPoolExecutor pool = new ThreadPoolExecutor(4, 8, 3, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(200),
    //        new ThreadPoolExecutor.AbortPolicy());
	static String TrimURL(String url)
	{
		if(url==null)
			return null;
		int last;
		if(url.charAt(url.length()-1)=='>')
		{
			last=url.length()-1;
		}
		else
		{
			last=url.length();
		}
		return url.substring(url.lastIndexOf('/')+1,last);
	}
	
	
	static class MyTask
	{
		String name,type,url;		
		int id;
		public MyTask(String url,String name,String type,int id)
		{
			if(url!=null && url.length()>256)
			{
				System.out.printf("url %s too long\n",url);
				url=url.substring(0, 256);
			}
			if(name!=null && name.length()>60000)
			{
				System.out.printf("name %s too long\n",name);
				name=name.substring(0, 256);
			}
			if(type!=null && type.length()>128)
			{
				System.out.printf("type %s too long\n",type);
				type=type.substring(0, 128);	
			}
			
			this.url=url;
			this.name=name;
			this.type=type;
			this.id=id;
		
		}
	}
	
	static private final class StringTask implements Callable<String> {
		MyTask[] task;
		int count;
		static AtomicInteger cnt=new AtomicInteger(0);
		static AtomicInteger pending_cnt=new AtomicInteger(0);
		public StringTask(MyTask[] tsk,int cnt)
		{
			count=cnt;
			task=tsk;
		}
		public String call() {
			// Long operations
			PreparedStatement stmt;
			try {
				StringBuffer buf=new StringBuffer();
				buf.append("INSERT INTO main VALUES (?,?,?,?,0)");
				for(int i=1;i<count;i++)
				{
					buf.append(",(?,?,?,?,0)");
				}
				stmt = con.prepareStatement(buf.toString());
			
				for(int i=0;i<count;i++)
				{
					stmt.setString(i*4+1, task[i].url);
					stmt.setString(i*4+2, task[i].name);
					stmt.setString(i*4+3, task[i].type);
					stmt.setInt(i*4+4, task[i].id);
					task[i]=null;
				}
				task=null;
				
				stmt.executeUpdate();
				cnt.incrementAndGet();
				pending_cnt.decrementAndGet();

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
			return null;
		}
	}
	
	static Thread statth = new Thread() {
		@Override
		public void run() {	
			for(;;)
			{
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ThreadDeath e)
				{
					
				}
			
				System.out.printf("[stats] i = %d, tasksdone = %d, task_queued = %d\n",
						readth.i,StringTask.cnt.get(),StringTask.pending_cnt.get());
			}
		}
	};
	
	static class ReaderThread extends  Thread {
		String url = null; 
		String name,type;		
		int id=0;
		public int i = 0;
		MyTask[] tsk;
		int tsk_cnt=0;
		final int TASKS=65536/4;
		
		void parse(String[] line)
		{
			//System.out.println("1111");
			if(line[1].equals("<http://rdf.freebase.com/ns/type.object.name>"))
			{
				if(line[2].endsWith("\"@en"))
				{
					name=line[2].substring(1, line[2].length()-4);
				}
			}
			else if(line[1].equals("<http://rdf.freebase.com/ns/type.object.type>"))
			{
				type=line[2];
			}
		}	
		
		@Override
		public void run() {
			BufferedReader reader = new BufferedReader(new InputStreamReader(pis));
			

			int lastq=0;
			for (;;) {
				i++;
				
				String line = null;
				try {
					line = reader.readLine();
					if(line.isEmpty())
					{
						System.out.printf("Almost done, %d\n",tsk_cnt);
						StringTask.pending_cnt.incrementAndGet();
						pool.submit(new StringTask(tsk,tsk_cnt));
						break;
					}
					String[] sp=line.split("\t");
					parse(sp);
					if(!sp[0].equals(url))
					{
						if(url==null)
						{
							url=sp[0];
							tsk_cnt=0;
							tsk=new MyTask[TASKS];
						}
						else
						{
							while (StringTask.pending_cnt.get() > 10) {

								System.out.println("[warning] Queue too long, now sleeping");
								try {
									Thread.sleep(5000);
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}

							}
							MyTask ntsk=new MyTask(TrimURL(url),name,TrimURL(type),id);
							tsk[tsk_cnt]=ntsk;
							tsk_cnt++;
							if(tsk_cnt==TASKS)
							{
								StringTask.pending_cnt.incrementAndGet();
								pool.submit(new StringTask(tsk,tsk_cnt));
								tsk_cnt=0;
								tsk=new MyTask[TASKS];
							}
							url=sp[0];
							id++;
							name=null;
							type=null;
						}
					}
					parse(sp);
					
				}
				catch (IOException e) {
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
	static ReaderThread readth = new ReaderThread();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			try {
				//create table main( url varchar(256), name varchar(60000), type varchar(128), id int(32), rank int(32));

				Class.forName("com.mysql.jdbc.Driver").newInstance(); // MYSQL驱动
				con = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/master", "root", "thisismysql"); // 链接本地MYSQL
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
			statth.start();
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

		while ((count = gis.read(data, 0, 4096)) != -1) {
			pos.write(data, 0, count);
		}
		pos.write("\n".getBytes());
		gis.close();
	}
}
