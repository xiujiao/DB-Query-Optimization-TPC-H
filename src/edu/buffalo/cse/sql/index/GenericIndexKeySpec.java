
package edu.buffalo.cse.sql.index;

import java.util.Comparator;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.Schema;

public class GenericIndexKeySpec implements IndexKeySpec {

  Schema.Type[] dataSchema;
  int[] keyCols;
  int keyorder;//revised by lei 0509
//  public GenericIndexKeySpec(Schema.Type[] dataSchema, int[] keyCols,int keyorder)
//  {
//    this.dataSchema = dataSchema;
//    this.keyCols = keyCols;
//    this.keyorder=keyorder;
//  }
  public GenericIndexKeySpec(Schema.Type[] dataSchema, int leadingKeys,int leadingkeys1)
  {
//    this(dataSchema, leadingKeys(leadingKeys), leadingkeys1);
	  int[] keys = new int[leadingKeys];
	    for(int i = 0; i < leadingKeys; i++){
	      keys[i] = i;
	    }
	    this.dataSchema = dataSchema;
	    this.keyCols = keys;
	    this.keyorder=leadingkeys1;
  }
  
//  protected static int[] leadingKeys(int leading){
//    int[] keys = new int[leading];
//    for(int i = 0; i < leading; i++){
//      keys[i] = i;
//    }
//    return keys;
//  }

  public Datum[] createKey(Datum[] row)
  {
    Datum[] key = new Datum[keyCols.length]; 
    for(int i = 0; i < keyCols.length; i++){
      key[i] = row[keyCols[i]];
    }
    return key;
  }
  // revised by lei create key by order 0509 2013
  public Datum[] createKeybyorder(Datum[] row)
  {
    Datum[] key = new Datum[1]; 
//    for(int i = 0; i < keyCols.length; i++){
      key[0] = row[0];// the first column is the key revised by lei
//    }
    return key;
  }
  public int hashKey(Datum[] key)
  {
    return Datum.hashOfRow(key);
  }
  public int hashRow(Datum[] row)
  {
    return Datum.hashOfRow(createKey(row));
  }
  public int compare(Datum[] a, Datum[] b){
    return Datum.compareRows(a, b);
  }
  public Schema.Type[] rowSchema()
    { return dataSchema; }
  public Schema.Type[] keySchema()
  {
    Schema.Type[] keySchema = new Schema.Type[keyCols.length];
    for(int i = 0; i < keyCols.length; i++){
      keySchema[i] = dataSchema[keyCols[i]];
    } 
    return keySchema;
  }
}