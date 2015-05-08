/**
 *
 * @author  Renato Stoffalette Joao
 * @version 1.0
 * @since   2015-05 
 */
package org.myown.l3s;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class RemoveFiles{

	public static void main(String[] args) throws Exception {
		    String master = "hdfs://master.ib:8020";
		    final long start = System.currentTimeMillis();
		    int count = 0;
		    try {
				final Configuration conf = new Configuration();
				conf.set("fs.defaultFS", master);
				final FileSystem dfs = FileSystem.get(conf);
				RemoteIterator<LocatedFileStatus> iter = dfs.listFiles(new Path("/user/joao/FTDdeTA"), true);
	    		String path;
	    		while (iter.hasNext()) {
	    			LocatedFileStatus ls = iter.next();
	    			path = ls.getPath().toString();
	    			if(dfs.exists(new Path(path))){
	    				//I only want to delete files that are compressed .gz and with size 20 bytes
	    				if( (path.endsWith(".gz")) && (ls.getLen() == 20) ){
	    					count++;
	    					dfs.delete(new Path(path), true);
	    					//System.out.println(path + " " + ls.getLen());	
	    				}	    			    
	    			}
	    		}
		    } catch (final Exception e) {
				System.err.println(e);
				e.printStackTrace();
			}

		    final long stop = System.currentTimeMillis();
		    System.err.println("Finished deleting " + count + " files in " + ((stop - start)/1000) + " seconds");

		}
	
	}

