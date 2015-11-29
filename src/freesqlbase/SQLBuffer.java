package freesqlbase;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import freesqlbase.FreeSQLBase.SQLTask;
import freesqlbase.FreeSQLBase.SQLTask2;
import freesqlbase.FreeSQLBase.StringTask;

public class SQLBuffer{
	final int TASKS=16*1024;
	SQLTask[] tsk_et=new SQLTask[TASKS];
	SQLTask[] tsk_te=new SQLTask[TASKS];
	SQLTask[] tsk_other=new SQLTask[TASKS];
	SQLTask2[] tsk_str=new SQLTask2[TASKS];
	SQLTask[] tsk_rank=new SQLTask[TASKS];
	SQLTask2[] tsk_entity=new SQLTask2[TASKS];
	SQLTask[] tsk_content=new SQLTask[TASKS];
	SQLTask2[] tsk_source=new SQLTask2[TASKS];
	
	int tsk_cnt_et=0;
	int tsk_cnt_te=0;
	int tsk_cnt_other=0;
	int tsk_cnt_str=0;
	int tsk_cnt_rank=0;	
	int tsk_cnt_entity=0;
	int tsk_cnt_content=0;
	int tsk_cnt_source=0;
	boolean readdone=false;

	PreparedStatement getStmt4(SQLTask2[] task,int count)
	{
		PreparedStatement stmt=null;
		try {
			StringBuffer buf=new StringBuffer();
			buf.append("INSERT INTO entity VALUES (?,?,?)");
			for(int i=1;i<count;i++)
			{
				buf.append(",(?,?,?)");
			}
			stmt = FreeSQLBase.con.prepareStatement(buf.toString());
		
			for(int i=0;i<count;i++)
			{
				stmt.setInt(i*3+1, task[i].id);
				stmt.setString(i*3+2, task[i].str);
				stmt.setInt(i*3+3, task[i].id2);
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return stmt;
	}		
	
	
	PreparedStatement getStmt3(SQLTask[] task,int count)
	{
		PreparedStatement stmt=null;
		try {
			StringBuffer buf=new StringBuffer();
			buf.append("UPDATE main SET rank = CASE id\n WHEN ? THEN ?\n");
	        
	    //WHERE id IN (1,2,3)");
			for(int i=1;i<count;i++)
			{
				buf.append("when ? then ?\n");
			}
			buf.append("end\nwhere id in (?");
			for(int i=1;i<count;i++)
			{
				buf.append(",?");
			}
			buf.append(")");				
			stmt = FreeSQLBase.con.prepareStatement(buf.toString());
		
			for(int i=0;i<count;i++)
			{
				stmt.setInt(i*2+1, task[i].id1);
				stmt.setInt(i*2+2, task[i].id2);
			}
			for(int i=0;i<count;i++)
			{
				stmt.setInt(count*2+1+i, task[i].id1);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return stmt;

	}
	
	PreparedStatement getStmt2(SQLTask2[] task,int count,String name)
	{
		PreparedStatement stmt=null;
		try {
			StringBuffer buf=new StringBuffer();
			buf.append("INSERT INTO "+name+"  VALUES (?,?)");
			for(int i=1;i<count;i++)
			{
				buf.append(",(?,?)");
			}
			stmt = FreeSQLBase.con.prepareStatement(buf.toString());
		
			for(int i=0;i<count;i++)
			{
				stmt.setInt(i*2+1, task[i].id);
				stmt.setString(i*2+2, task[i].str);
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return stmt;
	}		
	
	PreparedStatement getStmt(SQLTask[] task,int count,String name)
	{
		PreparedStatement stmt=null;
		try {
			StringBuffer buf=new StringBuffer();
			buf.append("INSERT INTO ");
			buf.append(name);
			buf.append(" VALUES (?,?)");
			for(int i=1;i<count;i++)
			{
				buf.append(",(?,?)");
			}
			stmt = FreeSQLBase.con.prepareStatement(buf.toString());
		
			for(int i=0;i<count;i++)
			{
				stmt.setInt(i*2+1, task[i].id1);
				stmt.setInt(i*2+2, task[i].id2);
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return stmt;
	}
	
	void put_entity(SQLTask2 t)
	{
		synchronized(tsk_entity)
		{
			tsk_entity[tsk_cnt_entity]=t;
			tsk_cnt_entity++;
			if(tsk_cnt_entity==TASKS)
			{
				
				FreeSQLBase.pool.submit(new StringTask(getStmt4(tsk_entity,tsk_cnt_entity)));
				tsk_cnt_entity=0;
				tsk_entity=new SQLTask2[TASKS];
			}
		}			
	}	

	void put_content(SQLTask t)
	{
		synchronized(tsk_content)
		{
			tsk_content[tsk_cnt_content]=t;
			tsk_cnt_content++;
			if(tsk_cnt_content==TASKS)
			{
				
				FreeSQLBase.pool.submit(new StringTask(getStmt(tsk_content,tsk_cnt_content,"content")));
				tsk_cnt_content=0;
				tsk_content=new SQLTask[TASKS];
			}
		}			
	}
	
	void put_source(SQLTask2 t)
	{
		synchronized(tsk_source)
		{
			tsk_source[tsk_cnt_source]=t;
			tsk_cnt_source++;
			if(tsk_cnt_source==TASKS)
			{
				
				FreeSQLBase.pool.submit(new StringTask(getStmt2(tsk_source,tsk_cnt_source,"source")));
				tsk_cnt_source=0;
				tsk_source=new SQLTask2[TASKS];
			}
		}			
	}
	
	void put_rank(SQLTask t)
	{
		synchronized(tsk_rank)
		{
			tsk_rank[tsk_cnt_rank]=t;
			tsk_cnt_rank++;
			if(tsk_cnt_rank==TASKS)
			{
				
				FreeSQLBase.pool.submit(new StringTask(getStmt3(tsk_rank,tsk_cnt_rank)));
				tsk_cnt_rank=0;
				tsk_rank=new SQLTask[TASKS];
			}
		}			
	}
	
	void put_str(SQLTask2 t)
	{
		synchronized(tsk_str)
		{
			tsk_str[tsk_cnt_str]=t;
			tsk_cnt_str++;
			if(tsk_cnt_str==TASKS)
			{
				
				FreeSQLBase.pool.submit(new StringTask(getStmt2(tsk_str,tsk_cnt_str,"strings")));
				tsk_cnt_str=0;
				tsk_str=new SQLTask2[TASKS];
			}
		}
	}
	

	void put_other(SQLTask t)
	{
		synchronized(tsk_other)
		{
			tsk_other[tsk_cnt_other]=t;
			tsk_cnt_other++;
			if(tsk_cnt_other==TASKS)
			{
				
				FreeSQLBase.pool.submit(new StringTask(getStmt(tsk_other,tsk_cnt_other,"other")));
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
				FreeSQLBase.pool.submit(new StringTask(getStmt(tsk_te,tsk_cnt_te,"type_entity")));
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
				FreeSQLBase.pool.submit(new StringTask(getStmt(tsk_et,tsk_cnt_et,"entity_type")));
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
		System.out.printf("flush tsk_cnt_te=%d,tsk_cnt_et=%d,tsk_cnt_other=%d,tsk_cnt_str=%d,tsk_cnt_rank=%d,tsk_cnt_entity=%d,tsk_cnt_content=%d,tsk_cnt_source=%d\n"
				,tsk_cnt_te,tsk_cnt_et,tsk_cnt_other,tsk_cnt_str,tsk_cnt_rank,tsk_cnt_entity,tsk_cnt_content,tsk_cnt_source);
		synchronized(tsk_te)
		{
			if(tsk_cnt_te!=0)
			{
				FreeSQLBase.pool.submit(new StringTask(getStmt(tsk_te,tsk_cnt_te,"type_entity")));	
				tsk_cnt_te=0;
				tsk_te=null;
			}
		}
		synchronized(tsk_et)
		{
			if(tsk_cnt_et!=0)
			{
				FreeSQLBase.pool.submit(new StringTask(getStmt(tsk_et,tsk_cnt_et,"entity_type")));
				tsk_cnt_et=0;
				tsk_et=null;
			}
		}
		synchronized(tsk_other)
		{
			if(tsk_cnt_other!=0)
			{
				FreeSQLBase.pool.submit(new StringTask(getStmt(tsk_other,tsk_cnt_other,"other")));
				tsk_cnt_other=0;
				tsk_other=null;
			}
		}
		synchronized(tsk_str)
		{
			if(tsk_cnt_str!=0)
			{
				FreeSQLBase.pool.submit(new StringTask(getStmt2(tsk_str,tsk_cnt_str,"strings")));
				tsk_cnt_str=0;
				tsk_str=null;
			}
		}
		synchronized(tsk_rank)
		{
			if(tsk_cnt_rank!=0)
			{
				FreeSQLBase.pool.submit(new StringTask(getStmt3(tsk_rank,tsk_cnt_rank)));
				tsk_cnt_rank=0;
				tsk_rank=null;
			}
		}
		synchronized(tsk_entity)
		{
			if(tsk_cnt_entity!=0)
			{
				FreeSQLBase.pool.submit(new StringTask(getStmt4(tsk_entity,tsk_cnt_entity)));
				tsk_cnt_entity=0;
				tsk_entity=null;
			}
		}
		synchronized(tsk_content)
		{
			if(tsk_cnt_content!=0)
			{
				FreeSQLBase.pool.submit(new StringTask(getStmt(tsk_content,tsk_cnt_content,"content")));
				tsk_cnt_content=0;
				tsk_content=null;
			}
		}
		synchronized(tsk_source)
		{
			if(tsk_cnt_source!=0)
			{
				FreeSQLBase.pool.submit(new StringTask(getStmt2(tsk_source,tsk_cnt_source,"source")));
				tsk_cnt_source=0;
				tsk_source=null;
			}
		}
	}
}
