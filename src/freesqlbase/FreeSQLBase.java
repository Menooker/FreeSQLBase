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
//124519884
//123893488
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
		final int SIZE=1024*1024;
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
		
		synchronized int get(String s) throws KeyNotFoundException
		{
			try
			{
				int i=s.hashCode();
				if(i<0)
					i=-i;
				i=i % SIZE;
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
			catch (Exception e)
			{
				e.printStackTrace();
				throw new KeyNotFoundException(s);
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
			finally{
				pending_cnt.decrementAndGet();
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
		int id;
		static AtomicInteger pending=new AtomicInteger(0);
		public EntryTask(String[] lne,int i){
			line=lne;
			id=i;
			pending.incrementAndGet();
		}
		@Override
		public String call() {
			int id1,id2;
			int id_from_2=-1;
			try {
				if(line[1].equals("<http://rdf.freebase.com/ns/type.object.type>"))
				{
					id1=id;
					id2=sqlcache.get(TrimURL(line[2]));
					id_from_2=id2;
					sqlbuf.put_et(new SQLTask(id1,id2));
					
				}
				else if(line[1].equals("<http://rdf.freebase.com/ns/type.property.expected_type>") 
						|| line[1].equals("<http://rdf.freebase.com/ns/type.property.schema>"))
				{
					id1=sqlcache.get(TrimURL(line[2]));
					id2=id;
					id_from_2=id1;
					sqlbuf.put_te(new SQLTask(id1,id2));	
				}
				else if(line[0].startsWith("<http://rdf.freebase.com/ns/") && line[0].charAt(29)=='.')
				{//if it is an entity
					if(id_from_2==-1)
						id_from_2=sqlcache.get(TrimURL(line[2]));
					if(id<=id_from_2)
						sqlbuf.put_other(new SQLTask(id,id_from_2));
				}
				
			} catch (KeyNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally
			{
				pending.decrementAndGet();
			}
			if(pending.get()<=0 && sqlbuf.readdone)
			{
				System.out.println("Flush buffer");
				sqlbuf.flush();
			}
			return null;
		}
		
	}
	
	static class SQLBuffer{
		final int TASKS=16*1024;
		SQLTask[] tsk_et=new SQLTask[TASKS];
		SQLTask[] tsk_te=new SQLTask[TASKS];
		SQLTask[] tsk_other=new SQLTask[TASKS];
		int tsk_cnt_et=0;
		int tsk_cnt_te=0;
		int tsk_cnt_other=0;
		boolean readdone=false;

		void put_other(SQLTask t)
		{
			synchronized(tsk_other)
			{
				tsk_other[tsk_cnt_other]=t;
				tsk_cnt_other++;
				if(tsk_cnt_other==TASKS)
				{
					pool.submit(new StringTask(tsk_other,tsk_cnt_other,"other"));
					tsk_cnt_other=0;
					tsk_other=new SQLTask[TASKS];
				}
			}
		}
		
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
		
		void done()
		{
			readdone=true;
		}
		
		void flush()
		{
			System.out.printf("flush %d %d %d\n",tsk_cnt_te,tsk_cnt_et,tsk_cnt_other);
			synchronized(tsk_te)
			{
				if(tsk_cnt_te!=0)
				{
					pool.submit(new StringTask(tsk_te,tsk_cnt_te,"type_entity"));	
					tsk_cnt_te=0;
					tsk_te=null;
				}
			}
			synchronized(tsk_et)
			{
				if(tsk_cnt_et!=0)
				{
					pool.submit(new StringTask(tsk_et,tsk_cnt_et,"entity_type"));
					tsk_cnt_et=0;
					tsk_et=null;
				}
			}
			synchronized(tsk_other)
			{
				if(tsk_cnt_other!=0)
				{
					pool.submit(new StringTask(tsk_other,tsk_cnt_other,"other"));
					tsk_cnt_other=0;
					tsk_other=null;
				}
			}
		}
	}
	

	
	static class ReaderThread extends  Thread {

		public long i = 0;
		public int id=0;

		

		@Override
		public void run() {
			BufferedReader reader = new BufferedReader(new InputStreamReader(pis));
			String last=null;
			int id=-1;
			for (;;) {
				i++;
				
				String line = null;
				try {
					line = reader.readLine();
					if(line.isEmpty())
					{
						System.out.printf("Almost done\n");
						sqlbuf.done();
						if(EntryTask.pending.get()==0)
						{
							System.out.println("Main thread Flush");
							sqlbuf.flush();
						}
						//statth.stop();
						break;
					}
					
					while(EntryTask.pending.get()>500000 
							|| StringTask.pending_cnt.get()>300)
					{
						//System.out.println("Queue too long, sleeping...");
						try {
							Thread.sleep(500);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					String[] sp=line.split("\t");
					///////////////////////////////////////////////
					if(!sp[0].equals(last))
					{
						last=sp[0];
						id++;
					}
					//if(id<=124519884)
					//	continue;
					///////////////////////////////////////////////				
					linepool.submit(new EntryTask(sp,id));				
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
				//create table other(e_id int(32), t_id int(32), primary key(e_id,t_id));
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
					new File("/home/menooker/freebase.gz")); 
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
