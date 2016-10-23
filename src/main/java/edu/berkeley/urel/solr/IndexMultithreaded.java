package edu.berkeley.urel.solr;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ResourceBundle;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IndexMultithreaded {
	private static Logger logger = LoggerFactory.getLogger(IndexMultithreaded.class);
	
	private IndexMultithreaded() {
	}

	public static void main(String... args) throws Exception {
		
		String solrHome = ResourceBundle.getBundle("edu.berkeley.urel.solr.config").getString("solrhome");
		File solrxmlFile = new File(solrHome, "solr.xml");
		File confDir = new File(solrHome, "conf");
		CoreContainer coreContainer = CoreContainer.createAndLoad(solrHome, solrxmlFile);
		
		int numberOfCores = Integer.parseInt(ResourceBundle.getBundle("edu.berkeley.urel.solr.config").getString("numberOfCores"));
		for(int i=0; i<numberOfCores;i++){	
			File coreConfDir =  new File(solrHome+"/core"+i+"/conf");
			coreConfDir.mkdirs();
			FileUtils.copyDirectory(confDir, coreConfDir);
		}
		
		List<Indexer> indexers = new ArrayList<Indexer>();
		for(int i=0; i<numberOfCores;i++){	
			CoreDescriptor cd = new CoreDescriptor(coreContainer, "core"+i, solrHome+"/core"+i);
			coreContainer.create(cd);
			EmbeddedSolrServer server = new EmbeddedSolrServer(coreContainer, "core"+i);
			indexers.add(new Indexer(server, ResourceBundle.getBundle("edu.berkeley.urel.solr.config").getString("query")+i));
		}

		for(Indexer indexer : indexers){
			indexer.start();
		}
		
		
		// Wait will they are all finished before exiting
		
		for(Indexer i: indexers) {
			try {
				if(i.isAlive()) {
					i.join();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		
		logger.info("End Time: " + new Date());
		System.exit(0);
		

		//Merge indexes
		//java -cp /Users/ogergardtadmin/.m2/repository/org/apache/lucene/lucene-core/5.2.0/lucene-core-5.2.0.jar:/Users/ogergardtadmin/.m2/repository/org/apache/lucene/lucene-misc/5.2.0/lucene-misc-5.2.0.jar org/apache/lucene/misc/IndexMergeTool /Users/ogergardtadmin/Documents/solrtest/name/data/index /Users/ogergardtadmin/Documents/solrtest/name0/data/index /Users/ogergardtadmin/Documents/solrtest/name1/data/index /Users/ogergardtadmin/Documents/solrtest/name2/data/index /Users/ogergardtadmin/Documents/solrtest/name3/data/index
	}


}
