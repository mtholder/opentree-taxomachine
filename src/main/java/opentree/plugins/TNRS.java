package opentree.plugins;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

import opentree.TaxonomyExplorer;
import opentree.tnrs.TNRSAdapter;
import opentree.tnrs.TNRSMatch;
import opentree.tnrs.TNRSMatchSet;
import opentree.tnrs.TNRSQuery;
import opentree.tnrs.adapters.iplant.TNRSAdapteriPlant;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.ObjectToRepresentationConverter;

public class TNRS extends ServerPlugin {

	@Description ("Return information on potential matches to a search query")
	@PluginTarget (GraphDatabaseService.class)
	public Representation doTNRSForNames(@Source GraphDatabaseService graphDb,
            @Description("A comma-delimited string of names to be queried upon")
			@Parameter(name = "queryString") String queryString) {
	    
//		String response = "{\"test\":\"response\"}";
        String[] searchStrings = queryString.split("\\s*\\,\\s*");

        TaxonomyExplorer taxonomy = new TaxonomyExplorer();
        taxonomy.setDbService(graphDb);
        TNRSAdapter iplant = new TNRSAdapteriPlant();
        
        Map responseM = new HashMap();
        String response = "{\"results\":[";
        boolean first = true;
        for (int i = 0; i < searchStrings.length; i++) {
            String[] queriedName = new String[1]; // TODO: should perhaps change this to accept only a single string instead of an array.
            queriedName[0] = searchStrings[i];
            if (first)
                first = false;
            else
                response += ",";

            response += "{\"queried_name\":\"" + queriedName[0] + "\",\"matches\":[";
            TNRSQuery tnrs = new TNRSQuery(taxonomy);
            TNRSMatchSet matches = tnrs.getMatches(queriedName, iplant);
            
            boolean first2 = true;
            ArrayList< Map<String, Object> >  match_list = new ArrayList();
            for (TNRSMatch m : matches) {
                match_list.add(m.getAttributes());
            }
            responseM.put("match", match_list);
            
            response += "]}";
        }
        response += "]}";
        
        taxonomy.shutdownDB();
//        Object [] obj_array = new Object[2];
  //      obj_array[0] = 10.1;
    //    obj_array[1] = 2.3;
        responseM.put("results", new Integer(1));
        return  ObjectToRepresentationConverter.convert( responseM );
	}
/*
	@Description ("Return information on potential matches to a search query")
	@PluginTarget (GraphDatabaseService.class)
	public String doTNRSForNames(@Source GraphDatabaseService graphDb,
            @Description("A comma-delimited string of names to be queried upon")
			@Parameter(name = "queryString") String queryString) {
	    
//		String response = "{\"test\":\"response\"}";
        String[] searchStrings = queryString.split("\\s*\\,\\s*");

        TaxonomyExplorer taxonomy = new TaxonomyExplorer();
        taxonomy.setDbService(graphDb);
        TNRSAdapter iplant = new TNRSAdapteriPlant();
        
        String response = "{\"results\":[";
        boolean first = true;
        for (int i = 0; i < searchStrings.length; i++) {
            String[] queriedName = new String[1]; // TODO: should perhaps change this to accept only a single string instead of an array.
            queriedName[0] = searchStrings[i];
            if (first)
                first = false;
            else
                response += ",";

            response += "{\"queried_name\":\"" + queriedName[0] + "\",\"matches\":[";
            TNRSQuery tnrs = new TNRSQuery(taxonomy);
            TNRSMatchSet matches = tnrs.getMatches(queriedName, iplant);

            boolean first2 = true;
            for (TNRSMatch m : matches) {
                if (first2)
                    first2 = false;
                else
                    response += ",";                
                response += "{";
                
                response += "\"matched_node_id\":" + m.getMatchedNodeId() + ",";
                response += "\"matched_node_name\":\"" + m.getMatchedNodeName() + "\",";
///                response += "\"synonym_node_id\":" + m.getSynonymNodeId() + "\",";
//             response += "\"synonym_node_name\":\"" + m.getSynonymNodeName() + "\","; 
                response += "\"source\":\"" + m.getSource() + "\",";
                response += "\"is_exact_node\":\"" + m.getIsExactNode() + "\",";
                response += "\"is_approximate_node\":\"" + m.getIsApproximate() + "\",";
                response += "\"is_synonym\":\"" + m.getIsSynonym() + "\",";
                response += "\"is_homonym\":\"" + m.getIsHomonym() + "\",";
                response += "\"score\":" + m.getScore();
                
                // TODO: add addtional data for external matches, or maybe just a URI for these pointing to the original source

                response += "}";
            }
            
            response += "]}";
        }
        response += "]}";
        
        taxonomy.shutdownDB();		
        return response;
	}
	*/
	
/*	@Description ("Return a JSON with the node id given a name")
	@PluginTarget (GraphDatabaseService.class)
	public String getNodeIDJSONFromName(@Source GraphDatabaseService graphDb,
			@Description("Name of node to find.")
			@Parameter( name = "nodename", optional= true ) String nodename ){
		String retst = "";
		System.out.println(nodename);
		IndexHits<Node> hits = graphDb.index().forNodes("taxNamedNodes").get("name",nodename);
		try{
			Node firstNode = hits.next();
			hits.close();
			if(firstNode == null){
				retst = "[]";
			}else{
				retst="[{\"nodeid\":"+firstNode.getId()+"}]";
			}
		}catch(java.lang.Exception jle){
			retst = "[]";
		}
		return retst;
	} */
}
