package edu.buffalo.cse.sql.index;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.DatumBuffer;
import edu.buffalo.cse.sql.data.DatumSerialization;
import edu.buffalo.cse.sql.data.InsufficientSpaceException;
import edu.buffalo.cse.sql.io.Readtable;
import edu.buffalo.cse.sql.io.Readtable_old;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.BufferException;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.parser.ParseException;
import edu.buffalo.cse.sql.test.TestDataStream;

public class ISAMIndex implements IndexFile {

	private static final int Inf = 0;
	ManagedFile mf1;
	IndexKeySpec key1;

	public ISAMIndex(ManagedFile file, IndexKeySpec keySpec)
			throws IOException, SqlException {
		this.mf1 = file;
		this.key1 = keySpec;
	}

	public static ISAMIndex create(FileManager fm, File path,
			Iterator<Datum[]> dataSource, IndexKeySpec key, String pathfile, int keyorder)
			throws SqlException, IOException {

		int i = 0;
		double re = 1; // number of record
		int le = 0; // length of one record
		int re1 = 0;// how many records one page can hold
		int pa = 0; // number of pages
		int start = 0;
		int end = 0;
		TestDataStream tds = (TestDataStream) dataSource;
		String[][] records=new String [1][1];
		try {
			records = tds.getTable(pathfile);//this funciton is wrtten by lei
//			Readtable_old readr= new Readtable_old();
//			String [][] caonima1=readr.readfile("l_shipdate.txt");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}// record all the records
		
		//only build index based on the "keyorder column"
		int [][] onecolumn=new int[records.length][2];
		for (int o1=0;o1<records.length;o1++){
 			if (records[o1][0].contains("-")){
				records[o1][0]=records[o1][0].replace("-", "");
			}
			onecolumn[o1][0]=Integer.parseInt(records[o1][0]);
			onecolumn[o1][1]=Integer.parseInt(records[o1][1]);//the second column records the order of the row, then later when the results are gotten by the index (first column), the second column can be used to reach the real whole data
		}
		//sort 
//		
//		for (int o2 = 0; o2 < onecolumn.length ; o2++) {
//			for (int o3 = 0; o3 < onecolumn.length - 1; o3++) {
//				int[] ss;
//				if (onecolumn[o3][0]>onecolumn[o3 + 1][0]) {
//					ss = onecolumn[o3];
//					onecolumn[o3] = onecolumn[o3 + 1];
//					onecolumn[o3 + 1] = ss;
//					
//				}
//			}
//			System.out.println(o2);
//		}
		
		// save to file, next time wont nedd save again
//		File tofile = new File("p_partkey.txt");
//		FileWriter fw;
//		try {
//			fw = new FileWriter(tofile, true);
//			
////			fw.write(Integer.toString(1));
////			fw.write(",");
////			fw.write(Integer.toString(2));
////			fw.write("\r\n");
//			for (int o4=0;o4<onecolumn.length;o4++){
//			fw.write(Integer.toString(onecolumn[o4][0]));
//			fw.write(",");
//			fw.write(Integer.toString(onecolumn[o4][1]));
//			fw.write("\r\n");}
//			fw.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

		
		
		
		File path1 = new File("index1.dat");
		ManagedFile mf1 = fm.open(path1);
		mf1.resize(1);

		// Iterator<Datum []> dataSource1=new Iterator <Datum []>;

		Schema.Type[] sch = tds.getSchema();
		// Datum [] d1=tds.next();
		// Datum [] d11=new Datum [sch.length];
		// d11=d1;
		// records.add(d1);

		int le1 = onecolumn[0].length;// length of one record
		Datum[] d1 = new Datum[le1];
		for (int dd = 0; dd < le1; dd = dd + 1) {
			d1[dd] = new Datum.Int(onecolumn[0][dd]); // transfer from str to
													// datum
		}
		Datum[] k1 = key.createKeybyorder(d1);
		int le2 = k1.length;// length of the key
		Datum d22 = d1[le2];// 0-le2-1 is the key, le2 is the mark of the tree
							// level
		for (int ddd=0;ddd<le2;ddd++){
			d1[ddd]=k1[ddd];
		}
		d1[le2] = new Datum.Int(0);// replace the original value in record
									// (useless) and mark it as the lowest level
									// in the tree

		DatumBuffer db = new DatumBuffer(mf1.getBuffer(0), sch);
		db.initialize(le1 * 4);// reserved space
		db.writereserve(d1);
		d1[le2] = d22;
		// Datum [] d2=db.readreserve();
		i = db.remaining();

		// Datum[] get =new Datum [10];
		// get[0]=new Datum.Int(0);
		// get[1]=new Datum.Int(0);
		// get[2]=new Datum.Int(0);
		// get[3]=new Datum.Int(0);
		// get[4]=new Datum.Int(0);
		// get[5]=new Datum.Int(0);
		// get[6]=new Datum.Int(0);
		// get[7]=new Datum.Int(0);
		// get[8]=new Datum.Int(0);
		// get[9]=new Datum.Int(0);

		tds = (TestDataStream) dataSource;
		// while (tds.hasNext()){
		// // if (re==6789)
		// // {
		// // int re2=0;
		// // }
		// d1=tds.next();
		// // Datum [] d2=db.readreserve();
		// records.add(d1);
		// if (i-8>(d1.length+1)*4){
		// re1=0;}
		// while (i-8>(d1.length+1)*4){
		// db.write(d1);
		// d1=db.read(0);
		// i=db.remaining();
		// re1=re1+1;}
		//
		// le=d1.length;
		// re=re+1;
		//
		// }
		re = onecolumn.length;// records number
		int l1=d1.length;
		while (i - 8 > (d1.length + 1) * 4) {
			l1=d1.length;
			db.write(d1);
			d1 = db.read(0);
			i = db.remaining();
			re1 = re1 + 1;
		}// records number per page
		double pa1 = Math.ceil(re / re1);
		pa = (int) pa1;// how many pages are necessary
		fm.close(path1);

		// ///////////////////////////////////////////the upper part is to
		// calculate pa//////////////
		ManagedFile mf = fm.open(path);
		mf.resize(pa);

		// TestDataStream tds1=(TestDataStream)dataSource;
		// sch=tds.getSchema();

		int level = 0;// mark the level of the tree
		int i3 = 0;
		for (int i1 = 0; i1 < pa; i1++) {
			DatumBuffer db1 = new DatumBuffer(mf.getBuffer(i1), sch);
			db1.initialize(le1 * 4);
			int flag = 0;
			for (int j1 = 0; j1 < re1; j1++) {
				if (i3 < re) {
					for (int dd = 0; dd < le1; dd = dd + 1) {
						d1[dd] = new Datum.Int(onecolumn[i3][dd]); // transfer
																	// from str
																	// to datum
					}
					if (flag == 0) { // the reserved space will store the key of
										// the first record now
						k1 = key.createKeybyorder(d1);
						for (int ddd=0;ddd<le2;ddd++){
							d1[ddd]=k1[ddd];
						}
						d22 = d1[le2];
						d1[le2] = new Datum.Int(level);
						db1.writereserve(d1);

						DatumBuffer db4 = new DatumBuffer(mf.getBuffer(0), sch);
						Datum[] d5 = db4.readreserve();
						// int cmp=key.compare(get, d5);
						// int pro=0;
						// if (cmp==0)
						// {
						// pro=1;
						// }
						// else if (cmp>1)
						// {
						// pro=1;
						// }

						for (int dd = 0; dd < le1; dd = dd + 1) {
							d1[dd] = new Datum.Int(onecolumn[i3][dd]); // transfer
																		// from str
																		// to datum
						}
						flag = 1;
					}
					db1.write(d1);
					i3 = i3 + 1;
				} else {
					break;
				}
			}
			mf.dirty(i1);
			mf.flush();

		}
		start = 0;
		end = pa - 1;

		// DatumBuffer db4=new DatumBuffer(mf.getBuffer(0),sch);
		// int si=db4.length();
		// Datum [] d5=db4.read(15);
		// DatumBuffer db5=new DatumBuffer(mf.getBuffer(1),sch);
		// Datum [] d6=db5.read(0);
		// DatumBuffer db6=new DatumBuffer(mf.getBuffer(30),sch);
		// Datum [] d7=db6.readreserve();

		int pa2 = pa;
		while (pa2 - 1 > re1) {
			// DatumBuffer db3=new DatumBuffer(mf.getBuffer(0),sch);
			// Datum [] d2=db3.readreserve();

			pa1 = Math.ceil((pa1 - 1) / re1);// (pa1-1) is the number of keys
												// information for the lower
												// level of the tree
			pa2 = (int) pa1;// pa1 is the extra pages needed
			mf.resize(pa + pa2);
			level = level + 1;
			i3 = 0;
			for (int i1 = pa; i1 < pa + pa2; i1++) {
				DatumBuffer db1 = new DatumBuffer(mf.safeGetBuffer(i1), sch);
				// DatumBuffer db2=new DatumBuffer(mf.getBuffer(i1),sch);
				db1.initialize(le1 * 4);
				int flag = 0;
				// i3=i3+1;

				for (int j1 = 0; j1 < re1; j1++) {
					if (i3 < end - start + 1) {
						int ire2 = db1.remaining();
						// DatumBuffer db2=new
						// DatumBuffer(mf.getBuffer(start+i3),sch);
						// mf.pin(start+i3);
						DatumBuffer db2 = new DatumBuffer(
								mf.safeGetBuffer(start + i3), sch);
						// mf.pin(start+i3);///////////////////////////////////////this
						// is very important, because when the frames are only
						// set as 10, it will crash when 9 pages are open
						// without being set free
						int ire3 = db1.remaining();
						d1 = db2.readreserve();

						db2.initialize();
						// mf.unpin(start+i3);///////////////////////////////////////this
						// is very important, because when the frames are only
						// set as 10, it will crash when 9 pages are open
						// without being set free
						// mf.unpin(start+i3);
						// mf.flush();
						ire3 = db1.remaining();

						// DatumBuffer db23=new
						// DatumBuffer(mf.safeGetBuffer(start+i3),sch);
						// Datum [] d12=db2.readreserve();
						int ire4 = db1.remaining();// read the first keys from the lower level and change the read keys'level by adding 1
						if (flag == 0) {
							d22 = d1[le2];
							d1[le2] = new Datum.Int(level);
							db1.writereserve(d1);
							// d1=db1.readreserve();
							Datum[] dr = db1.readreserve();
							d1[le2] = d22;
							flag = 1;
						}
						int ire = db1.remaining();
						db1.write(d1);
						DatumBuffer db24 = new DatumBuffer(mf.getBuffer(i1),
								sch);
						d1 = db24.readreserve();
						int ire1 = db1.remaining();
						i3 = i3 + 1;
					} else {
						break;
					}
				}
				DatumBuffer db24 = new DatumBuffer(mf.getBuffer(i1), sch);
				d1 = db24.readreserve();
				mf.dirty(i1);
				mf.flush();

				// DatumBuffer db44=new DatumBuffer(mf.getBuffer(0),sch);
				// Datum [] d55=db44.readreserve();
				// int cmp=key.compare(get, d55);
				// int pro=0;
				// if (cmp==0)
				// {
				// pro=1;
				// }
				// else if (cmp>1)
				// {
				// pro=1;
				// }

			}
			start = pa;
			end = pa + pa2 - 1;
			pa = pa + pa2;

		}

		// db4=new DatumBuffer(mf.getBuffer(0),sch);
		// d5=db4.readreserve();

		if (pa2 - 1 < re1) {// the last time updates 
			level = level + 1;
			mf.resize(pa + 1);
			i3 = 0;
			for (int i1 = pa; i1 < pa + 1; i1++) {
				DatumBuffer db1 = new DatumBuffer(mf.getBuffer(i1), sch);

				db1.initialize(le1 * 4);
				int flag = 0;
				// i3=i3+1;

				for (int j1 = 0; j1 < re1; j1++) {
					if (i3 < end - start + 1) {

						// mf.pin(start+i3);
						DatumBuffer db2 = new DatumBuffer(mf.getBuffer(start
								+ i3), sch);
						d1 = db2.readreserve();
						db2.initialize();
						// mf.unpin(start+i3);

						if (flag == 0) {
							d22 = d1[le2];
							d1[le2] = new Datum.Int(level);
							db1.writereserve(d1);
							flag = 1;
							d1[le2] = d22;
						}
						db1.write(d1);
						i3 = i3 + 1;
					} else {
						break;
					}
				}
				mf.dirty(i1);
				mf.flush();
			}
		}
		// db4=new DatumBuffer(mf.getBuffer(0),sch);
		// d5=db4.readreserve();
		// db4=new DatumBuffer(mf.getBuffer(667),sch);
		// d5=db4.readreserve();
		// db4=new DatumBuffer(mf.getBuffer(712),sch);
		// d5=db4.readreserve();
		// db4=new DatumBuffer(mf.getBuffer(715),sch);
		// d5=db4.readreserve();

		// for (int i4=0; i4<mf.size();i4++){
		// mf.dirty(i4);}
		// db4=new DatumBuffer(mf.getBuffer(0),sch);
		// d5=db4.readreserve();
		// mf.flush();
		// DatumBuffer db4=new DatumBuffer(mf.getBuffer(715),sch);
		// Datum [] d5=db4.readreserve();
		fm.close(path);
		ISAMIndex I1 = new ISAMIndex(mf, key);
		return I1;

		// throw new SqlException("Unimplemented");
	}

	public Iterator<Datum[]> scan() throws SqlException, IOException {
		ArrayList<Datum[]> al = new ArrayList<Datum[]>();

		int pa = mf1.size();
		int root = 0;
		for (int i = 0; i < pa; i++) {
			// mf1.pin(i);
			DatumBuffer rootBuffer = new DatumBuffer(mf1.getBuffer(i),
					key1.rowSchema());
			Datum[] rootDatum = rootBuffer.readreserve();
			Datum[] k2 = key1.createKeybyorder(rootDatum);
			int node = rootDatum[k2.length].toInt();
			if (node == root) {
				for (int j = 0; j < rootBuffer.length(); j++) {
					Datum[] re = rootBuffer.read(j);
					al.add(re);
				}
			}
			if (node > root) {
				break;
			}
		}

		Iterator<Datum[]> in = al.iterator();

		// IndexIterator ini=(IndexIterator)in;
		return in;
	}
	public Iterator<Datum[]> rangeScanTo(Datum[] toKey) throws SqlException,
			IOException {
		ArrayList<Datum[]> al = new ArrayList<Datum[]>();
		Datum[] toKey1 = this.getTo(toKey);

		int pa = mf1.size();
		int root = 0;
		for (int i = 0; i < pa; i++) {
			// mf1.pin(i);
			DatumBuffer rootBuffer = new DatumBuffer(mf1.getBuffer(i),
					key1.rowSchema());
			Datum[] rootDatum = rootBuffer.readreserve();
			Datum[] k2 = key1.createKeybyorder(rootDatum);
			int cmp = key1.compare(k2, toKey1);
			int node = rootDatum[k2.length].toInt();
			if (node == root && cmp <= 0) {
				for (int j = 0; j < rootBuffer.length(); j++) {
					Datum[] re = rootBuffer.read(j);
					Datum[] k3 = key1.createKeybyorder(re);
					int cmp1 = key1.compare(k3, toKey1);
					if (cmp1 <= 0) { // check the records in the same page
						al.add(re);
					} else if (cmp1 > 0) {
						break;
					}
				}
			}
			if (cmp > 0) {
				break;
			}
		}

		Iterator<Datum[]> in = al.iterator();
		// IndexIterator ini=(IndexIterator)in;
		return in;
	}
	public Iterator<Datum[]> rangeScanFrom(Datum[] fromKey)
			throws SqlException, IOException {
		ArrayList<Datum[]> al = new ArrayList<Datum[]>();
		Datum[] fromKey1 = this.getFrom(fromKey);

		int pa = mf1.size();
		int root = 0;
		int flag = 1;
		for (int i = 0; i < pa; i++) {
			// mf1.pin(i);
			DatumBuffer rootBuffer = new DatumBuffer(mf1.getBuffer(i),
					key1.rowSchema());
			Datum[] rootDatum = rootBuffer.readreserve();
			Datum[] k2 = key1.createKeybyorder(rootDatum);
			int node = rootDatum[k2.length].toInt();

			int cmp = key1.compare(k2, fromKey1);
			if (node == root && cmp >= 0 && flag == 1 && i > 0) {
				DatumBuffer rootBuffer1 = new DatumBuffer(mf1.getBuffer(i - 1),
						key1.rowSchema());
				for (int j = 0; j < rootBuffer1.length(); j++) {
					Datum[] re = rootBuffer1.read(j);
					Datum[] k3 = key1.createKeybyorder(re);
					int cmp1 = key1.compare(k3, fromKey1);
					if (cmp1 >= 0) {
						al.add(re);
					}
				}
				flag = 0;
			} else if (node == root && cmp >= 0 && flag == 1 && i == 0) {
				flag = 0;
			} else if (node == root && cmp < 0 && i > 1) {
				DatumBuffer rootBuffer1 = new DatumBuffer(mf1.getBuffer(i),
						key1.rowSchema());
				for (int j = 0; j < rootBuffer1.length(); j++) {
					Datum[] re = rootBuffer1.read(j);
					Datum[] k3 = key1.createKeybyorder(re);
					int cmp1 = key1.compare(k3, fromKey1);
					if (cmp1 >= 0) {
						al.add(re);
					}
				}
				flag=0;//flag=0 means that the first page where the first record appears has been found 05092013 by lei

			}

			if (node == root && cmp >= 0) {
				for (int j = 0; j < rootBuffer.length(); j++) {
					Datum[] re = rootBuffer.read(j);
					Datum[] k3 = key1.createKeybyorder(re);
					int cmp1 = key1.compare(k3, fromKey1);
					al.add(re);
				}
			}
			if (node > root) {
				break;
			}
		}

		Iterator<Datum[]> in = al.iterator();
		// IndexIterator ini=(IndexIterator)in;
		return in;
	}
	
	public Iterator<Datum[]> rangeScan(Datum[] start, Datum[] end) throws SqlException, IOException {
		ArrayList<Datum[]> al = new ArrayList<Datum[]>();
		Datum[] fromKey1 = this.getFrom(start);
		Datum[] toKey1 = this.getTo(end);

		int pa = mf1.size();
		int root = 0;
		int flag = 1;

		for (int i = 0; i < pa; i++) {
			// mf1.pin(i);
			DatumBuffer rootBuffer = new DatumBuffer(mf1.getBuffer(i),
					key1.rowSchema());
			Datum[] rootDatum = rootBuffer.readreserve();
			Datum[] k2 = key1.createKeybyorder(rootDatum);
			int cmpfrom = key1.compare(k2, fromKey1);
			int cmpto = key1.compare(k2, toKey1);

			int node = rootDatum[k2.length].toInt();

			if (node == root && cmpfrom >= 0 && flag == 1 && i > 0) {
				DatumBuffer rootBuffer1 = new DatumBuffer(mf1.getBuffer(i - 1),
						key1.rowSchema());
				for (int j = 0; j < rootBuffer1.length(); j++) {
					Datum[] re = rootBuffer1.read(j);
					Datum[] k3 = key1.createKeybyorder(re);
					int cmpfrom1 = key1.compare(k3, fromKey1);
					int cmpto1 = key1.compare(k3, toKey1);
					if (cmpfrom1 >= 0 && cmpto1 <= 0) {
						al.add(re);
					}
				}
				flag = 0;
			} else if (node == root && cmpfrom >= 0 && flag == 1 && i == 0) {
				flag = 0;
			} else if (node == root && cmpfrom < 0 && i > 1) {
				DatumBuffer rootBuffer1 = new DatumBuffer(mf1.getBuffer(i),
						key1.rowSchema());
				for (int j = 0; j < rootBuffer1.length(); j++) {
					Datum[] re = rootBuffer1.read(j);
					Datum[] k3 = key1.createKeybyorder(re);
					int cmp1 = key1.compare(k3, fromKey1);
					if (cmp1 >= 0) {
						al.add(re);
					}
				}
				flag=0;//flag=0 means that the first page where the first record appears has been found 05092013 by lei

			}

			if (node == root && cmpfrom >= 0 && cmpto <= 0) {
				for (int j = 0; j < rootBuffer.length(); j++) {
					Datum[] re = rootBuffer.read(j);
					Datum[] k3 = key1.createKeybyorder(re);
					int cmpfrom1 = key1.compare(k3, fromKey1);
					int cmpto1 = key1.compare(k3, toKey1);
					if (cmpfrom1 >= 0 && cmpto1 <= 0) {
						al.add(re);
					}
				}
			}
			if (node > root) {
				break;
			}
		}

		Iterator<Datum[]> in = al.iterator();
//		Datum [] cao=in.next();
		// IndexIterator ini=(IndexIterator)in;
		return in;
	}
	public Datum[] getTo(Datum[] key) throws SqlException, IOException {
		int pa = mf1.size();// size of pages
		int root = 0;// the depth of the tree
		Datum[] d = null;

		DatumBuffer rootBuffer = new DatumBuffer(mf1.getBuffer(pa - 1),
				key1.rowSchema());
		Datum[] rootDatum = rootBuffer.readreserve();
		Datum[] k1 = key1.createKeybyorder(rootDatum);
		root = rootDatum[k1.length].toInt();
		Datum[] min = new Datum[k1.length];
		for (int j = 0; j < k1.length; j++) // initialize the min to record the
											// most closest key to the wanted
											// key
		{
			min[j] = new Datum.Int(0);
		}

		int flag = -1;
		for (int i = pa - 1; i >= 0; i--) {
			rootBuffer = new DatumBuffer(mf1.getBuffer(i), key1.rowSchema());
			rootDatum = rootBuffer.readreserve();
			Datum[] k2 = key1.createKeybyorder(rootDatum);
			int node = rootDatum[k1.length].toInt();
			if (node == root) {
				int cmp = key1.compare(key, k2);
				if (cmp == 1) {
					int cmp1 = key1.compare(k2, min);
					if (cmp1 == 1 || cmp1 == 0) {
						min = k2; // find the most closest key in the current
									// level of the tree to the wanted key
						flag = i;
					}
				} else if (cmp == 0) {
					min = k2;
					flag = i;
					break;
				}
			}
			if (node < root) {
				break;
			}
		}
		if (flag != -1) {
			rootBuffer = new DatumBuffer(mf1.getBuffer(flag), key1.rowSchema());
			for (int i1 = 0; i1 < rootBuffer.length(); i1++) {
				rootDatum = rootBuffer.read(i1);
				Datum[] k2 = key1.createKeybyorder(rootDatum);
				int cmp = key1.compare(key, k2);
				if (cmp == 1) {
					int cmp1 = key1.compare(k2, min);
					if (cmp1 == 1 || cmp1 == 0) {
						min = k2; // find the most closest key in the next level
									// of the tree to the wanted key
					}
				} else if (cmp == 0) {
					min = k2;
					break;
				}
			}
		}

		root = root - 1;
		for (root = root; root >= 0; root--) {
			flag = -1;
			Datum[] k3 = min;
			min = new Datum[k1.length];
			for (int j = 0; j < k1.length; j++) {
				min[j] = new Datum.Int(0);
			}
			for (int i = pa - 1; i >= 0; i--) {
				rootBuffer = new DatumBuffer(mf1.getBuffer(i), key1.rowSchema());
				rootDatum = rootBuffer.readreserve();
				Datum[] k2 = key1.createKeybyorder(rootDatum);
				int node = rootDatum[k1.length].toInt();
				if (node == root) {
					int cmp = key1.compare(k3, k2);
					if (cmp == 0) // here is a little different,because this
									// must exist
					{
						flag = i; // find the most closest key in the current
									// level of the tree to the wanted key
						break;
					}
				}
				if (node < root) {
					break;
				}
			}
			if (flag != -1) {
				rootBuffer = new DatumBuffer(mf1.getBuffer(flag),
						key1.rowSchema());
				int i11 = rootBuffer.length();
				for (int i1 = 0; i1 < rootBuffer.length(); i1++) {
					rootDatum = rootBuffer.read(i1);
					Datum[] k2 = key1.createKeybyorder(rootDatum);
					int cmp = key1.compare(key, k2);
					if (cmp == 1) {
						int cmp1 = key1.compare(k2, min);
						if (cmp1 == 1 || cmp1 == 0) {
							min = k2; // find the most closest key in the next
										// level of the tree to the wanted key
						}
					} else if (cmp == 0) {
						min = k2;
						break;
					}
				}

			}
		}
		d = min;
		return d;
		// throw new SqlException("Unimplemented");
	}
	public Datum[] getFrom(Datum[] key) throws SqlException, IOException {

		int pa = mf1.size();// size of pages
		int root = 0;// the depth of the tree
		Datum[] d = null;

		DatumBuffer rootBuffer = new DatumBuffer(mf1.getBuffer(pa - 1),
				key1.rowSchema());
		Datum[] rootDatum = rootBuffer.readreserve();
		Datum[] k1 = key1.createKeybyorder(rootDatum);
		root = rootDatum[k1.length].toInt();
		Datum[] min = new Datum[k1.length];
		for (int j = 0; j < k1.length; j++) // initialize the min to record the
											// most closest key to the wanted
											// key
		{
			min[j] = new Datum.Int(0);
		}

		int flag = -1;
		for (int i = pa - 1; i >= 0; i--) {
			rootBuffer = new DatumBuffer(mf1.getBuffer(i), key1.rowSchema());
			rootDatum = rootBuffer.readreserve();
			Datum[] k2 = key1.createKeybyorder(rootDatum);
			int node = rootDatum[k1.length].toInt();
			if (node == root) {
				int cmp = key1.compare(key, k2);
				if (cmp == 1) {
					int cmp1 = key1.compare(k2, min);
					if (cmp1 == 1 || cmp1 == 0) {
						min = k2; // find the most closest key in the current
									// level of the tree to the wanted key
						flag = i;
					}
				} else if (cmp == 0) {
					min = k2;
					flag = i;
					break;
				}
			}
			if (node < root) {
				break;
			}
		}
		if (flag != -1) {
			rootBuffer = new DatumBuffer(mf1.getBuffer(flag), key1.rowSchema());
			for (int i1 = 0; i1 < rootBuffer.length(); i1++) {
				rootDatum = rootBuffer.read(i1);
				Datum[] k2 = key1.createKeybyorder(rootDatum);
				int cmp = key1.compare(key, k2);
				if (cmp == 1) {
					int cmp1 = key1.compare(k2, min);
					if (cmp1 == 1 || cmp1 == 0) {
						min = k2; // find the most closest key in the next level
									// of the tree to the wanted key
					}
				} else if (cmp == 0) {
					min = k2;
					break;
				}
			}
		}

		root = root - 1;
		for (root = root; root >= 1; root--) {
			flag = -1;
			Datum[] k3 = min;
			min = new Datum[k1.length];
			for (int j = 0; j < k1.length; j++) {
				min[j] = new Datum.Int(0);
			}
			for (int i = pa - 1; i >= 0; i--) {
				rootBuffer = new DatumBuffer(mf1.getBuffer(i), key1.rowSchema());
				rootDatum = rootBuffer.readreserve();
				Datum[] k2 = key1.createKeybyorder(rootDatum);
				int node = rootDatum[k1.length].toInt();
				if (node == root) {
					int cmp = key1.compare(k3, k2);
					if (cmp == 0) // here is a little different,because this
									// must exist
					{
						flag = i; // find the most closest key in the current
									// level of the tree to the wanted key
						break;
					}
				}
				if (node < root) {
					break;
				}
			}
			if (flag != -1) {
				rootBuffer = new DatumBuffer(mf1.getBuffer(flag),
						key1.rowSchema());
				int i11 = rootBuffer.length();
				for (int i1 = 0; i1 < rootBuffer.length(); i1++) {
					rootDatum = rootBuffer.read(i1);
					Datum[] k2 = key1.createKeybyorder(rootDatum);
					int cmp = key1.compare(key, k2);
					if (cmp == 1) {
						int cmp1 = key1.compare(k2, min);
						if (cmp1 == 1 || cmp1 == 0) {
							min = k2; // find the most closest key in the next
										// level of the tree to the wanted key
						}
					} else if (cmp == 0) {
						min = k2;
						break;
					}
				}

			}
		}
		Datum[] max = new Datum[k1.length];
		for (int j = 0; j < k1.length; j++) // initialize the min to record the
											// most closest key to the wanted
											// key
		{
			max[j] = new Datum.Int(1000000000);
		} // find the smallest key that are larger than the current key

		Datum[] k3 = min;
		flag = -1;
		for (int i = 0; i < pa; i++) {
			rootBuffer = new DatumBuffer(mf1.getBuffer(i), key1.rowSchema());
			rootDatum = rootBuffer.readreserve();
			Datum[] k2 = key1.createKeybyorder(rootDatum);
			int node = rootDatum[k1.length].toInt();
			if (node == 0) {
				int cmp = key1.compare(k3, k2);
				if (cmp == 0) // here is a little different,because this must
								// exist
				{
					flag = i; // find the most closest key in the current level
								// of the tree to the wanted key
					break;
				}
			}
		}
		if (flag != -1) {
			rootBuffer = new DatumBuffer(mf1.getBuffer(flag), key1.rowSchema());
			int i11 = rootBuffer.length();
			for (int i1 = 0; i1 < rootBuffer.length(); i1++) {
				rootDatum = rootBuffer.read(i1);
				Datum[] k2 = key1.createKeybyorder(rootDatum);
				int cmp = key1.compare(key, k2);
				if (cmp == -1) {
					int cmp1 = key1.compare(k2, max);
					if (cmp1 == -1 || cmp1 == 0) {
						max = k2; // find the smallest key that are larger than
									// the current key
					}
				} else if (cmp == 0) {
					max = k2;
					break;
				}
			}

		}
		if (flag == -1) {
			rootBuffer = new DatumBuffer(mf1.getBuffer(0), key1.rowSchema());
			rootDatum = rootBuffer.read(0);
			max = key1.createKeybyorder(rootDatum);
		}
		d = max;
		return d;
		// throw new SqlException("Unimplemented");
	}
	public Datum[] get(Datum[] key) throws SqlException, IOException {
		int pa = mf1.size();// size of pages
		int root = 0;// the depth of the tree
		Datum[] d = null;
		// int keyhash=key.hashCode();

		// Datum[] a=new Datum [2];
		// a[0]=new Datum.Int(2);
		// a[1]=new Datum.Int(4);

		// Datum[] b=new Datum [2];
		// b[0]=new Datum.Int(2);
		// b[1]=new Datum.Int(4);

		// int a1=a.hashCode();
		// int b1=b.hashCode();

		// int com=key1.compare(a,b);

		DatumBuffer rootBuffer1 = new DatumBuffer(mf1.getBuffer(0),
				key1.rowSchema());
		Datum[] rootDatum1 = rootBuffer1.readreserve();

		DatumBuffer rootBuffer = new DatumBuffer(mf1.getBuffer(pa - 1),
				key1.rowSchema());
		Datum[] rootDatum = rootBuffer.readreserve();
		Datum[] k1 = key1.createKeybyorder(rootDatum);
		root = rootDatum[k1.length].toInt();
		Datum[] min = new Datum[k1.length];
		for (int j = 0; j < k1.length; j++) // initialize the min to record the
											// most closest key to the wanted
											// key
		{
			min[j] = new Datum.Int(0);
		}

		int flag = -1;
		for (int i = pa - 1; i >= 0; i--) {
			rootBuffer = new DatumBuffer(mf1.getBuffer(i), key1.rowSchema());
			rootDatum = rootBuffer.readreserve();
			Datum[] k2 = key1.createKeybyorder(rootDatum);
			int node = rootDatum[k1.length].toInt();
			if (node == root) {
				int cmp = key1.compare(key, k2);
				if (cmp == 1) {
					int cmp1 = key1.compare(k2, min);
					if (cmp1 == 1 || cmp1 == 0) {
						min = k2; // find the most closest key in the current
									// level of the tree to the wanted key
						flag = i;
					}
				} else if (cmp == 0) {
					min = k2;
					flag = i;
					break;
				}
			}
			if (node < root) {
				break;
			}
		}
		if (flag != -1) {
			rootBuffer = new DatumBuffer(mf1.getBuffer(flag), key1.rowSchema());
			for (int i1 = 0; i1 < rootBuffer.length(); i1++) {
				rootDatum = rootBuffer.read(i1);
				Datum[] k2 = key1.createKeybyorder(rootDatum);
				int cmp = key1.compare(key, k2);
				if (cmp == 1) {
					int cmp1 = key1.compare(k2, min);
					if (cmp1 == 1 || cmp1 == 0) {
						min = k2; // find the most closest key in the next level
									// of the tree to the wanted key
					}
				} else if (cmp == 0) {
					min = k2;
					break;
				}
			}
		}

		root = root - 1;
		for (root = root; root >= 0; root--) {
			flag = -1;
			Datum[] k3 = min;
			min = new Datum[k1.length];
			for (int j = 0; j < k1.length; j++) {
				min[j] = new Datum.Int(0);
			}
			for (int i = pa - 1; i >= 0; i--) {
				rootBuffer = new DatumBuffer(mf1.getBuffer(i), key1.rowSchema());
				rootDatum = rootBuffer.readreserve();
				Datum[] k2 = key1.createKeybyorder(rootDatum);
				int node = rootDatum[k1.length].toInt();
				if (node == root) {
					int cmp = key1.compare(k3, k2);
					if (cmp == 0) // here is a little different,because this
									// must exist
					{
						flag = i; // find the most closest key in the current
									// level of the tree to the wanted key
						break;
					}
				}
				if (node < root) {
					break;
				}
			}
			if (flag != -1) {
				rootBuffer = new DatumBuffer(mf1.getBuffer(flag),
						key1.rowSchema());
				int i11 = rootBuffer.length();
				for (int i1 = 0; i1 < rootBuffer.length(); i1++) {
					rootDatum = rootBuffer.read(i1);
					Datum[] k2 = key1.createKeybyorder(rootDatum);
					int cmp = key1.compare(key, k2);
					if (cmp == 1) {
						int cmp1 = key1.compare(k2, min);
						if (cmp1 == 1 || cmp1 == 0) {
							min = k2; // find the most closest key in the next
										// level of the tree to the wanted key
						}
					} else if (cmp == 0) {
						min = k2;
						if (cmp == 0 && root == 0) {
							d = rootDatum;
						}
						break;
					}
				}

			}
		}
		return d;
		// throw new SqlException("Unimplemented");
	}

}
