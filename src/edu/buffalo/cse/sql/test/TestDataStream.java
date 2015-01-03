/**
 * A testing harness that generates arbitrary length data streams.  Thanks to a
 * hardcoded seed value, the data streams for one set of parameters (#keys, ...)
 * will be random, but identical each time.
 * 
 * You can vary the # of keys, the # of non-key values, and the the # of rows
 * generated.  Each data column will be an integer.  Keys are guaranteed to be
 * generated in monotonically increasing order, with keys on the left taking 
 * priority (in other words, the generated dataset is already sorted).
 * 
 * You can fine-tune the behavior of the key-incrementing process.  There is a
 * chaos parameter that controls how rapidly key-steps occur.  The higher this
 * value is, the more different adjacent keys will be.
 * 
 * The guaranteeKeyStep parameter, if true, will ensure that no two data rows
 * will have the same key.  If false, two data rows may have the same key -- 
 * especially for low values of the chaos parameter.
 *
 * TestDataStream also includes a pair of validation methods.  These allow you
 * to verify the accuracy of an index scan iterator that you provide.  Entries
 * in the provided iterator will be read off, invalid entries will be reported
 * as an error, and the method will return false
 **/

package edu.buffalo.cse.sql.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Iterator;
import java.io.*;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.io.Readtable;
import edu.buffalo.cse.sql.io.Readtable_old;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.parser.*;

public class TestDataStream implements Iterator<Datum[]> {

	int rows;
	int values;
	int[] curr;
	int chaos;
	boolean guaranteeKeyStep;
	Random rand;
//	static int cols;
	
	public TestDataStream(int keys, int values, int rows) {
		this(keys, values, rows, keys * 10, true);
	}

	public TestDataStream(int keys, int values, int rows, int chaos,
			boolean guaranteeKeyStep) {
		this.rows = rows;
		this.values = values;
		this.curr = new int[keys];
		this.chaos = chaos;
		this.guaranteeKeyStep = guaranteeKeyStep;
		for (int i = 0; i < this.curr.length; i++) {
			this.curr[i] = 0;
		}
		rand = new Random(52982);
	}

	// get tables from file 05092013 by Lei Lin
	public static String [][] getTable(String Path)
			throws SqlException, ParseException {
		//			ParserGrammar parse = new ParserGrammar(
		//					new java.io.FileInputStream(program));
					String arraytotal[][]=new String [0][0];
		//			Map<String, Schema.TableFromFile> tables = new HashMap<String, Schema.TableFromFile>();
		//			ArrayList<PlanNode> query=new ArrayList<PlanNode>();
		//			query=parse.PARSERSQL(tables);
		//			
		//			Schema.TableFromFile table_R=tables.get(0);
		//			  String pathadd=table_R.getFile().toString();
					Readtable_old readr= new Readtable_old();
				      arraytotal= readr.readfile(Path);
//				      cols=arraytotal[0].length;
					return arraytotal;
	}
	
	public Schema.Type[] getSchema() {
//		int cols = values + curr.length;
		Schema.Type[] sch = new Schema.Type[2];//only build index for "keyorder column"
		//the second column records the order of the row, then later when the results are gotten by the index (first column), the second column can be used to reach the real whole data
		for (int i = 0; i < 2; i++) {
			sch[i] = Schema.Type.INT;
		}
		return sch;
	}
//	public Schema.Type[] getSchema() {
//		int cols = values + curr.length;
//		Schema.Type[] sch = new Schema.Type[cols];
//		for (int i = 0; i < cols; i++) {
//			sch[i] = Schema.Type.INT;
//		}
//		return sch;
//	}

	public boolean validate(Iterator<Datum[]> inStream) {
		int i = 0;
		while (hasNext()) {
			if (!inStream.hasNext()) {
				System.err.println("Provided stream ended early");
				return false;
			}
			i++;
			Datum[] expected = next(), found = inStream.next();
			if (!Datum.rowEquals(expected, found)) {
				System.err.println("At Row: " + i);
				System.err.println("Expected: " + Datum.stringOfRow(expected));
				System.err.println("Found: " + Datum.stringOfRow(found));
				return false;
			}
		}
		if (inStream.hasNext()) {
			System.err.println("Provided stream is too long");
			return false;
		}
		return true;
	}

	public boolean validate(Iterator<Datum[]> inStream, Datum[] from, Datum[] to) {

		Datum[] expected = null, found;
		int i = 0;
		if (from != null) {
			while (hasNext()) {
				i++;
				expected = next();
				if (Datum.compareRows(from, expected) <= 0) {
					break;
				}
			}
		} else {
			if (hasNext()) {
				expected = next();
			}
		}
		if (expected == null) {
			return true;
		}

		int i1 = 0;
		while (hasNext()
				&& ((to == null) || (Datum.compareRows(expected, to) <= 0))) {
			i1 = i1 + 1;
			if (!inStream.hasNext()) {
				System.err.println("Provided stream ended early");
				return false;
			}
			i++;
			found = inStream.next();
			if (!Datum.rowEquals(expected, found)) {
				System.err.println("At Row: " + i);
				System.err.println("Expected: " + Datum.stringOfRow(expected));
				System.err.println("Found: " + Datum.stringOfRow(found));
				return false;
			}
			if (i == 9999) {
				i1 = 9999;
			}
			expected = next();

		}
		if (to == null) {
			if (!inStream.hasNext()) {
				System.err.println("Provided stream ended early");
				return false;
			}
			i++;
			found = inStream.next();// this is not compared with expected
			if (!Datum.rowEquals(expected, found)) {
				System.err.println("At Row: " + i);
				System.err.println("Expected: " + Datum.stringOfRow(expected));
				System.err.println("Found: " + Datum.stringOfRow(found));
				return false;
			}
		}
		if (inStream.hasNext()) {
			found = inStream.next();
			if (!hasNext() && Datum.compareRows(found, expected) == 0) {
			} // I think here there is a bug when expected is the last record in
				// the data stream, because hasnext() is false, it is not
				// compared with the found value
			else {
				System.err.println("Provided stream is too long");
				return false;
			}
		}
		return true;
	}

	protected void stepOneKey(boolean reset) {
		int tgt = rand.nextInt(curr.length);
		curr[tgt]++;
		if (reset) {
			for (int i = tgt + 1; i < curr.length; i++) {
				curr[i] = 0;
			}
		}
	}

	protected void stepAllKeys() {
		boolean reset = rand.nextInt(100) >= 70;
		int steps = rand.nextInt(chaos + 1);
		for (int i = guaranteeKeyStep ? 0 : 1; i <= steps; i++) {// dont think
																	// guaranteeKeyStep
																	// is true
																	// will
																	// guarantee
																	// this
																	// happen,
																	// false
																	// will
																	// happen
			stepOneKey(reset);
			reset = false;
		}
	}

	public boolean hasNext() {
		return rows > 0;
	}

	public Datum[] next()
	{
		Datum[] row = new Datum[values + curr.length];
		stepAllKeys();
		for (int i = 0; i < row.length; i++) {
			if (i < curr.length) {
				row[i] = new Datum.Int(curr[i]);

				File tofile = new File("record.txt");// revised by gaoxiujiao
				FileWriter fw;
				try {
					fw = new FileWriter(tofile, true);
					String str = row[i].toString();
					fw.write(str);
					fw.write(" ");
					fw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			} else {
				row[i] = new Datum.Int(rand.nextInt(1000));
				File tofile = new File("record.txt");
				FileWriter fw;
				try {
					fw = new FileWriter(tofile, true);
					String str = row[i].toString();
					fw.write(str);
					fw.write(" ");
					fw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		rows -= 1;
		File tofile = new File("record.txt");
		FileWriter fw;
		try {
			fw = new FileWriter(tofile, true);

			fw.write("\n ");
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return row;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
}