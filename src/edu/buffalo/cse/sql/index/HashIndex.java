
package edu.buffalo.cse.sql.index;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.DatumBuffer;
import edu.buffalo.cse.sql.data.DatumSerialization;
import edu.buffalo.cse.sql.data.InsufficientSpaceException;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.BufferException;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.test.TestDataStream;
 
public class HashIndex implements IndexFile {
  static ManagedFile  m_file;
  static IndexKeySpec idx_keySpec;
  
  public HashIndex(ManagedFile file, IndexKeySpec keySpec)
    throws IOException, SqlException
  {
	  this.m_file= file;
	  this.idx_keySpec = keySpec;
//    throw new SqlException("Unimplemented");
  }
  
  public static HashIndex create(FileManager fm,
                                 File path,
                                 Iterator<Datum[]> dataSource,
                                 IndexKeySpec key,
                                 int directorySize)
    throws SqlException, IOException
  {
	// take managed file from file manager according to file path
    path.delete();
	m_file = fm.open(path);

	// initialize the number of buckets to directory size
	m_file.resize(directorySize);
	
	Datum[] datarow = null;
	Schema.Type[] sch =null;
	// create index for dataSource data and store it into index file
	ArrayList flagini=null; 
	// initilize datum buffer for all the primary pages in the buckets
	for(int i =0;i<directorySize;i++)
	{
		ByteBuffer byte_buffer = m_file.safeGetBuffer(i);
		DatumBuffer datum_buffer = new DatumBuffer(byte_buffer,sch);
		datum_buffer.initialize(0);
//		Datum  datum_writepageid = new Datum.Int(i+directorySize);
//		DatumSerialization.write(byte_buffer, 0, datum_writepageid);
		m_file.dirty(i);
	}
	// All the primary pages's datum buffer are alreay initilized
	int temp=1;
	flagini = new ArrayList();
	flagini.add(temp);
	
	while(dataSource.hasNext())
	{
	// get row from data source
	  datarow=dataSource.next();
	  int record_length= DatumSerialization.getLength(datarow)+4;
	  sch = key.rowSchema();
	  // get key for the row, compute hash value and the index of the bucket it should be in 
	  Datum[] datakey=key.createKey(datarow);
	  int hashvalue= key.hashKey(datakey);
	  int bucketindex= hashvalue % directorySize;
	  int writepageid = bucketindex;
	  // store record directly in the bucket
	  // for each page except data record, we need to save an page id to indicate which flow page it may point to
	  ByteBuffer byte_buffer = m_file.safeGetBuffer(writepageid);
	  DatumBuffer datum_buffer = new DatumBuffer(byte_buffer,sch);
	  
	  int flag =1;
	  int flagini2= 1;
	  while(flag==1)
	  {
		  int reminding_size= datum_buffer.remaining();
		  if (datum_buffer.remaining() >= 2*record_length+8)
		  { 
			  int recordnum = datum_buffer.length();
			 datum_buffer.write(datarow);
			 recordnum = datum_buffer.length();
			
			 reminding_size=datum_buffer.remaining();
			 m_file.dirty(writepageid);
			 if(record_length+8 <= datum_buffer.remaining() && datum_buffer.remaining() <= 2*record_length+8)
			 {
				 Datum flowidflag = new Datum.Int(-1);
				 Datum  datum_writepageid = new Datum.Int(bucketindex+directorySize);
				 datarow[0] = flowidflag;
				 datarow[1] = datum_writepageid;
				 reminding_size=datum_buffer.remaining();
				 datum_buffer.write(datarow);
				 reminding_size=datum_buffer.remaining();
				 m_file.dirty(writepageid);
				 
			 }
//			 m_file.flush();
			 flag =0;
		  }else
		  {
			  flagini2++;
			  if(flagini.size()<flagini2 )
			  {
				  for(int i =writepageid;i<writepageid+directorySize;i++)
					{
						ByteBuffer byte_buffer2 = m_file.safeGetBuffer(i);
						DatumBuffer datum_buffer2 = new DatumBuffer(byte_buffer2,sch);
						datum_buffer2.initialize(0);
//						Datum  datum_writepageid = new Datum.Int(i+directorySize);
//						DatumSerialization.write(byte_buffer2, 0, datum_writepageid);
						m_file.dirty(i);
					}
				  flagini.add(flagini2);
			  }

			  writepageid+=directorySize;
			  ByteBuffer flowpage_byte_buffer = m_file.safeGetBuffer(writepageid);
			  DatumBuffer flowpage_datum_buffer = new DatumBuffer(flowpage_byte_buffer,sch);
			  
			  if (flowpage_datum_buffer.remaining() >= 2*record_length+8)
			  {
				 flowpage_datum_buffer.write(datarow);
				 reminding_size=datum_buffer.remaining();
				 m_file.dirty(writepageid);
				 if(record_length+8 <= flowpage_datum_buffer.remaining() && flowpage_datum_buffer.remaining() <= 2*record_length+8)
				 {
					 Datum flowidflag = new Datum.Int(-1);
					 Datum  datum_writepageid = new Datum.Int(bucketindex+directorySize);
					 datarow[0] = flowidflag;
					 datarow[1] = datum_writepageid;
					 reminding_size=flowpage_datum_buffer.remaining();
					 flowpage_datum_buffer.write(datarow);
					 reminding_size=flowpage_datum_buffer.remaining();
					 m_file.dirty(writepageid);
					 
				 }
//				 m_file.flush();
				 flag =0;
			  }
		  } 
	  }
	  
	}

    m_file.flush();
    
    ByteBuffer byte_buffer = m_file.safeGetBuffer(0);
	DatumBuffer datum_buffer = new DatumBuffer(byte_buffer,sch);
	int recordnum = datum_buffer.length(); 
	Datum[] getrecord = datum_buffer.read(recordnum-1);
	  if (getrecord[0].toInt() != -1)
	  {
		  Datum[] flowpageinfo =new Datum[2];
		  Datum flowidflag = new Datum.Int(-1);
		  Datum  datum_writepageid = new Datum.Int(directorySize);
		  
		  flowpageinfo[0]=flowidflag;
		  flowpageinfo[1]=datum_writepageid;
		  datum_buffer.write(flowpageinfo);
		  m_file.dirty(0);
		  m_file.flush();
	  }
	fm.close(path);
	
	// construct an object of itself, then return
	HashIndex hash_idx = new HashIndex(m_file,idx_keySpec);
	return hash_idx;
	
//    throw new SqlException("Unimplemented");
  }
  
  public IndexIterator scan() 
    throws SqlException, IOException
  {
    throw new SqlException("Unimplemented");
  }

  public IndexIterator rangeScanTo(Datum[] toKey)
    throws SqlException, IOException
  {
    throw new SqlException("Unimplemented");
  }

  public IndexIterator rangeScanFrom(Datum[] fromKey)
    throws SqlException, IOException
  {
    throw new SqlException("Unimplemented");
  }

  public IndexIterator rangeScan(Datum[] start, Datum[] end)
    throws SqlException, IOException
  {
    throw new SqlException("Unimplemented");
  }

  public Datum[] get(Datum[] key)
    throws SqlException, IOException
  {
	  Datum[] nullrow = null;
	  Schema.Type[] keySchem=idx_keySpec.keySchema();
	  if (keySchem.length != key.length)
	  {
		  return nullrow;
	  }
	  
	  Schema.Type[] rowSchem=idx_keySpec.rowSchema();

	  
	  ByteBuffer byte_buffer0 = m_file.safeGetBuffer(0);
	  DatumBuffer datum_buffer0 = new DatumBuffer(byte_buffer0,rowSchem);
//	  Schema.Type t;
//	  t= Schema.Type.INT;
	  Datum directorysize;
	  int bucketnum = 0;
	  for(int j =0; j< datum_buffer0.length();j++)
	  {
		  Datum[] getrecord = datum_buffer0.read(j);
		  if (getrecord[0].toInt() == -1)
		  {
			  directorysize = getrecord[1];
			  bucketnum= getrecord[1].toInt();
		  }
	  }

	  int pagenum = m_file.size();
	  int flowpagelevel = pagenum / bucketnum;
	  
      int hashvalue= idx_keySpec.hashKey(key);
	  int bucketindex= hashvalue % bucketnum;
	  int readpageid = bucketindex;
	  
	  for(int i=0; i<= flowpagelevel; i++)
	  {
		  ByteBuffer byte_buffer = m_file.safeGetBuffer(readpageid);
		  DatumBuffer datum_buffer = new DatumBuffer(byte_buffer,rowSchem);
		  int recordnum = datum_buffer.length();
		  for(int j =0; j< recordnum;j++)
		  {
			  Datum[] getrecord = datum_buffer.read(j);
			  Datum[] getrecordkey = new Datum[key.length];
			  for(int k =0;k<key.length;k++)
			  {
				  getrecordkey[k]= getrecord[k];
			  }
			  if(Datum.rowEquals(getrecordkey,key))
			  {return getrecord;}
		  }
		  readpageid+=bucketnum;
	  }
	 return nullrow;
	  
  }
    
}