package edu.berkeley.urel.solr;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestToCollection {
	private static Logger logger = LoggerFactory.getLogger(RequestToCollection.class);
	
	public static void main(String[] args) throws SolrServerException, IOException{
		String name = "name";
		String solrHome = ResourceBundle.getBundle("edu.berkeley.urel.solr.config").getString("solrhome");
		File solrxmlFile = new File(solrHome, "solr.xml");
		CoreContainer coreContainer = CoreContainer.createAndLoad(solrHome, solrxmlFile);
		CoreDescriptor cd = new CoreDescriptor(coreContainer, name, solrHome+"/"+name);
		coreContainer.create(cd);
		EmbeddedSolrServer server = new EmbeddedSolrServer(coreContainer, name);
		
		//request ALL
		ModifiableSolrParams solrParams = new ModifiableSolrParams();
		solrParams.add(CommonParams.Q, "*:*");
		QueryResponse queryResponse = server.query(solrParams);
		for (SolrDocument document : queryResponse.getResults()) {
			System.out.println(document);
		}
		
		server.close();
		logger.info("End Time: " + new Date());
		System.exit(0);
		
	}

}
