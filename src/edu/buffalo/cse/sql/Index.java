
package edu.buffalo.cse.sql;

import java.io.File;
import java.util.Iterator;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.index.IndexKeySpec;
import edu.buffalo.cse.sql.index.GenericIndexKeySpec;
import edu.buffalo.cse.sql.index.IndexFile;
import edu.buffalo.cse.sql.index.IndexIterator;
import edu.buffalo.cse.sql.index.ISAMIndex;
import edu.buffalo.cse.sql.index.HashIndex;
import edu.buffalo.cse.sql.test.TestDataStream;

public class Index {
  public enum IndexType { HASH, ISAM };
  
  public static Datum[] parseRow(String row){
    String[] fields = row.split("/, */");
    Datum[] ret = new Datum[fields.length];
    for(int i = 0; i < ret.length; i++){
      ret[i] = new Datum.Int(Integer.parseInt(fields[i]));
    }
    return ret;
  }
  
  public static void main(String[] args)
    throws Exception
  {
    IndexType type = IndexType.ISAM;
    int keys = 1;//the size of the key
    int values = 5;//the size of the value so keys+values size is the size of one row
    int rows = 100;
    int frames = 1024;//the size of neicun
    int keychaos = 2;//the randomness of the key
    int indexSize = 10;//for hash
    int keyorder=0;// create key by order
    String pathfile="test/lineitem.tbl";
    boolean validate = false;
//    Datum[] get =new Datum [1];
    Datum[] get=null;////////////////new Datum [10];//null; //// //new Datum [1];
    
//    get[0]=new Datum.Int(170);
//    get[1]=new Datum.Int(1);
//    get[2]=new Datum.Int(0);
//    get[3]=new Datum.Int(0);
//    get[4]=new Datum.Int(3);
//   get[5]=new Datum.Int(3);
//   get[6]=new Datum.Int(0);
//   get[7]=new Datum.Int(0);
//   get[8]=new Datum.Int(0);
//   get[9]=new Datum.Int(0);
   
    
   
//    Datum[] from = null;
    Datum [] from=new Datum [2];
  from[0]=new Datum.Int(-1);
  from[1]=new Datum.Int(1);
//  from[2]=new Datum.Int(0);
//  from[3]=new Datum.Int(0);
//  from[4]=new Datum.Int(0);
// from[5]=new Datum.Int(0);
// from[6]=new Datum.Int(0);
// from[7]=new Datum.Int(0);
// from[8]=new Datum.Int(0);
// from[9]=new Datum.Int(0);
//    Datum[] to = null;
    Datum[] to=new Datum [2];
    to[0]=new Datum.Int(192);
  to[1]=new Datum.Int(4);
//  to[2]=new Datum.Int(0);
////  to[3]=new Datum.Int(10);
//  to[4]=new Datum.Int(50);
//  to[5]=new Datum.Int(0);
//  to[6]=new Datum.Int(1);
//  to[7]=new Datum.Int(0);
//  to[8]=new Datum.Int(1);
//  to[9]=new Datum.Int(1);
////    
    
    File idxFile = new File("index.dat");
    
    for(int i = 0; i < args.length; i++){
      if(args[i].equals("-keys")){
        keys = Integer.parseInt(args[i+1]);
        i++; 
      } else if(args[i].equals("-values")){
        values = Integer.parseInt(args[i+1]);
        i++; 
      } else if(args[i].equals("-rows")){
        rows = Integer.parseInt(args[i+1]);
        i++; 
      } else if(args[i].equals("-frames")){
        frames = Integer.parseInt(args[i+1]);
        i++; 
      } else if(args[i].equals("-keychaos")){
        keychaos = Integer.parseInt(args[i+1]);
        i++; 
      } else if(args[i].equals("-validate")){
        validate = true;
      } else if(args[i].equals("-get")){
        get = parseRow(args[i+1]);
        i++; 
      } else if(args[i].equals("-from")){
        from = parseRow(args[i+1]);
        i++; 
      } else if(args[i].equals("-to")){
        to = parseRow(args[i+1]);
        i++; 
      } else if(args[i].equals("-indexSize")){
        indexSize = Integer.parseInt(args[i+1]);
        i++; 
      } else if(args[i].equals("-hash")){
        type = IndexType.HASH;
      } else if(args[i].equals("-isam")){
        type = IndexType.ISAM;
      } else {
        idxFile = new File(args[i]);
      }
    }
    
    BufferManager bm = new BufferManager(frames);
    FileManager fm = new FileManager(bm);
    
    TestDataStream ds = new TestDataStream(keys, values, rows, keychaos, true);
    IndexKeySpec keySpec = new GenericIndexKeySpec(ds.getSchema(), keys,keyorder);//revised
    
    if(validate){
      ManagedFile file = fm.open(idxFile);
      IndexFile idx = null;
      switch(type){
        case HASH:
          System.err.println("HASH Index scan validation unsupported");
          System.exit(-1);
          break;
        case ISAM:
          idx = new ISAMIndex(file, keySpec);
          break;
      }
      Iterator<Datum[]> scan;
      if(from == null){
        if(to == null){ scan = idx.scan(); }
        else { scan = idx.rangeScanTo(to); }
      } else {
        if(to == null){ scan = idx.rangeScanFrom(from); }
        else { scan = idx.rangeScan(from,to); }
      }
      try {
        if(ds.validate(scan, from, to)){
          System.out.println("Test Successful!");
          System.exit(0);
        } else {
          System.out.println("Test Failed!");
          System.exit(-1);
        }
      } finally {
//        scan.close();
      }
    } else if(get != null) {
      ManagedFile file = fm.open(idxFile);
      IndexFile idx = null;
      switch(type){
        case HASH:
          idx = new HashIndex(file, keySpec);
          break;
        case ISAM:
          idx = new ISAMIndex(file, keySpec);
          break;
      }
      
      System.out.println("Getting: "+Datum.stringOfRow(get));
      get = idx.get(get);
      System.out.println("Got: "+((get==null)?"Nothing"
                                             :Datum.stringOfRow(get)));
    
    } else {
      switch(type){
        case HASH:
          HashIndex.create(fm, idxFile, ds, keySpec, indexSize);
          break;
        case ISAM:
          ISAMIndex.create(fm, idxFile, ds, keySpec,pathfile,keyorder);//only build index for the keyorder column
          break;
      }
    }
    
    ManagedFile file = fm.open(idxFile);
    
  }
}