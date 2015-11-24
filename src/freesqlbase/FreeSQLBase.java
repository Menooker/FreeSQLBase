package freesqlbase;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
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
	static ExecutorService pool = Executors.newFixedThreadPool(2);
	static ExecutorService linepool = Executors.newFixedThreadPool(4);
	//static ThreadPoolExecutor pool = new ThreadPoolExecutor(4, 8, 3, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(200),
    //        new ThreadPoolExecutor.AbortPolicy());
	static SQLBuffer sqlbuf=new SQLBuffer();
	static SQLIdCache sqlcache=new SQLIdCache();
	
	static String TrimURL(String url)
	{
		if(url==null)
			return null;
		if(!url.startsWith("<http"))
			return url;
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
	
	static class KeyNotFoundException extends Exception
	{

		public KeyNotFoundException(String s) {
			super("SQL URL Not found: "+s);
			// TODO Auto-generated constructor stub
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = 433079364893439203L;
		
	}
	
	static class SQLIdCache
	{
		final int SIZE=65536;
		int[] cache=new int[SIZE];
		long hit=0;
		long cnt=1;
		SQLIdCache()
		{
			for(int i=0;i<SIZE;i++)
			{
				cache[i]=-1;
			}
		}
		
		int get(String s) throws KeyNotFoundException
		{
			int i=s.hashCode()%SIZE;
			cnt++;
			if(cache[i]!=-1)
			{
				hit++;
				return cache[i];
			}
			else
			{
				int ret=-1;
				PreparedStatement stmt = null;
				try {
					stmt=con.prepareStatement("select * from main where url=?");
					stmt.setString(1, s);
					ResultSet res = stmt.executeQuery();
					if (res.next()) {
						ret = res.getInt(4);
					}
					stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				cache[i]=ret;
				if(ret==-1)
					throw new KeyNotFoundException(s);
				return ret;

			}
		}
		
	}
	static class SQLTask
	{		
		int id1,id2;
		public SQLTask(int i1,int i2)
		{
			id1=i1;
			id2=i2;
		}
	}
	
	static private final class StringTask implements Callable<String> {
		SQLTask[] task;
		String name;
		int count;
		static AtomicInteger cnt=new AtomicInteger(0);
		static AtomicInteger pending_cnt=new AtomicInteger(0);
		static AtomicInteger interval_cnt=new AtomicInteger(0);
		
		public StringTask(SQLTask[] tsk,int cnt,String nme)
		{
			pending_cnt.incrementAndGet();
			interval_cnt.incrementAndGet();
			count=cnt;
			task=tsk;
			name=nme;
		}
		public String call() {
			pending_cnt.decrementAndGet();
			// Long operations
			PreparedStatement stmt;
			try {
				StringBuffer buf=new StringBuffer();
				buf.append("INSERT INTO ");
				buf.append(name);
				buf.append(" VALUES (?,?)");
				for(int i=1;i<count;i++)
				{
					buf.append(",(?,?)");
				}
				stmt = con.prepareStatement(buf.toString());
			
				for(int i=0;i<count;i++)
				{
					stmt.setInt(i*2+1, task[i].id1);
					stmt.setInt(i*2+2, task[i].id2);
					task[i]=null;
				}
				task=null;
				
				stmt.executeUpdate();
				stmt.close();
				cnt.incrementAndGet();
				
				
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
			
				System.out.printf("[stats] i = %d per = %f, tasksdone = %d, sql_queued = %d, line_queued = %d, hit_rate=%f, buf=%d, sql_tasks=%d\n",
						readth.i,1.0*readth.i/3130753067l,StringTask.cnt.get(),StringTask.pending_cnt.get(),EntryTask.pending.get(),
						1.0*sqlcache.hit/sqlcache.cnt,sqlbuf.tsk_cnt_te,StringTask.interval_cnt.get());
				sqlcache.hit=0;
				sqlcache.cnt=1;
				StringTask.interval_cnt.set(0);
			}
		}
	};
	
	
	static private final class EntryTask implements Callable<String> {
		String[] line;
		static AtomicInteger pending=new AtomicInteger(0);
		public EntryTask(String[] lne){
			line=lne;
			pending.incrementAndGet();
		}
		@Override
		public String call() {
			int id1,id2;
			pending.decrementAndGet();
			try {
				if(line[1].equals("<http://rdf.freebase.com/ns/type.object.type>"))
				{
					id1=sqlcache.get(TrimURL(line[0]));
					id2=sqlcache.get(TrimURL(line[2]));
					sqlbuf.put_et(new SQLTask(id1,id2));
				}
				else if(line[1].equals("<http://rdf.freebase.com/ns/type.property.expected_type>") 
						|| line[1].equals("<http://rdf.freebase.com/ns/type.property.schema>"))
				{
					id1=sqlcache.get(TrimURL(line[2]));
					id2=sqlcache.get(TrimURL(line[0]));
					sqlbuf.put_te(new SQLTask(id1,id2));	
				}
				
			} catch (KeyNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			return null;
		}
		
	}
	
	static class SQLBuffer{
		final int TASKS=16*1024;
		SQLTask[] tsk_et=new SQLTask[TASKS];
		SQLTask[] tsk_te=new SQLTask[TASKS];
		int tsk_cnt_et=0;
		int tsk_cnt_te=0;
		
		void put_te(SQLTask t)
		{
			synchronized(tsk_te)
			{
				tsk_te[tsk_cnt_te]=t;
				tsk_cnt_te++;
				if(tsk_cnt_te==TASKS)
				{
					pool.submit(new StringTask(tsk_te,tsk_cnt_te,"type_entity"));
					tsk_cnt_te=0;
					tsk_te=new SQLTask[TASKS];
				}
			}
		}
		
		void put_et(SQLTask t)
		{
			synchronized(tsk_et)
			{
				tsk_et[tsk_cnt_et]=t;
				tsk_cnt_et++;
				if(tsk_cnt_et==TASKS)
				{
					pool.submit(new StringTask(tsk_et,tsk_cnt_et,"entity_type"));
					tsk_cnt_et=0;
					tsk_et=new SQLTask[TASKS];
				}
			}
		}
		
		void flush()
		{
			synchronized(tsk_te)
			{
				pool.submit(new StringTask(tsk_te,tsk_cnt_te,"type_entity"));			
			}
			synchronized(tsk_et)
			{
				pool.submit(new StringTask(tsk_et,tsk_cnt_et,"enity_type"));
			}
		}
	}
	

	
	static class ReaderThread extends  Thread {

		public long i = 0;
		public int id=0;

		

		@Override
		public void run() {
			BufferedReader reader = new BufferedReader(new InputStreamReader(pis));
			
			for (;;) {
				i++;
				
				String line = null;
				try {
					line = reader.readLine();
					if(line.isEmpty())
					{
						System.out.printf("Almost done\n");
						sqlbuf.flush();
						//statth.stop();
						break;
					}
					while(EntryTask.pending.get()>500000 
							|| StringTask.pending_cnt.get()>300)
					{
						//System.out.println("Queue too long, sleeping...");
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					String[] sp=line.split("\t");
					linepool.submit(new EntryTask(sp));				
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
				//create table main( url varchar(256), name varchar(60000), type varchar(128), id int(32) primary key, rank int(32));
				//2*Maxint-1164214229=3130753067
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
