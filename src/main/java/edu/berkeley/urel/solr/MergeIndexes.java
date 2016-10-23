package edu.berkeley.urel.solr;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.misc.IndexMergeTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeIndexes {
	private static Logger logger = LoggerFactory.getLogger(MergeIndexes.class);
	
	public static void main(String[] args) throws IOException{
		String solrHome = "/Users/ogergardtadmin/Documents/solrtest2";
		//Create new collection <name>
		File coreConfDir =  new File(solrHome+"/name"+"/conf");
		coreConfDir.mkdirs();
		File confDir = new File(solrHome, "conf");
		FileUtils.copyDirectory(confDir, coreConfDir);
		
		//Merge all indexes to collection <name>
		int numberOfCores = 4;
		String[] indexes = new String[numberOfCores+1];
		indexes[0] = solrHome+"/name"+"/data/index";
		for(int i=1; i<numberOfCores+1;i++){	
			indexes[i]=solrHome+"/core"+(i-1)+"/data/index";
		}
		
		for(String i : indexes){
			System.out.println(i);
		}
		
		IndexMergeTool.main(indexes);
		FileUtils.forceDelete(new File(solrHome+"/name"+"/data/index/write.lock"));
		
		logger.info("End Time: " + new Date());
		System.exit(0);
	}

}
