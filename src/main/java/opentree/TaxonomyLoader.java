package opentree;


import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.*;
import org.apache.log4j.Logger;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * TaxonomyLoader is intended to control the initial creation 
 * and addition of taxonomies to the taxonomy graph.
 *
 */
public class TaxonomyLoader extends TaxonomyBase{
	static Logger _LOG = Logger.getLogger(TaxonomyLoader.class);
	int transaction_iter = 100000;
	int LARGE = 100000000;
	int globaltranscationnum = 0;
	Transaction gtx = null;
	
	//basic traversal method
	final TraversalDescription CHILDOF_TRAVERSAL = Traversal.description()
			.relationships( RelTypes.TAXCHILDOF,Direction.OUTGOING );
	
	/**
	 * Initializer assume that the graph is being used as embedded
	 * @param graphname directory path to embedded graph
	 */
	public TaxonomyLoader(String graphname){
		graphDb = new EmbeddedGraphDatabase( graphname );
		taxNodeIndex = graphDb.index().forNodes( "taxNodes" );
		prefTaxNodeIndex = graphDb.index().forNodes("prefTaxNodes");
		prefSynNodeIndex = graphDb.index().forNodes("prefSynNodes");
		synNodeIndex = graphDb.index().forNodes("synNodes");
		taxSourceIndex = graphDb.index().forNodes("taxSources");
	}
	
	
	/**
	 * Reads a taxonomy file with rows formatted as:
	 *	taxon_id\t|\tparent_id\t|\tName with spaces allowed\n
	 *
	 * Creates nodes and TAXCHILDOF relationships for each line.
	 * Nodes get a "name" property. Relationships get "source", "childid", "parentid" properties.
	 * 
	 * Nodes are indexed in taxNames "name" key and id value.
	 * 
	 * A metadata node is created to point to the root
	 * 
	 * The line that has no parent will be the root of this tree
	 * 
	 * @param sourcename this becomes the value of a "source" property in every relationship between the taxonomy nodes
	 * @param filename file path to the taxonomy file
	 * @param synonymfile file that holds the synonym
	 */
	public void initializeTaxonomyIntoGraph(String sourcename, String filename, String synonymfile){
		String str = "";
		int count = 0;
		Transaction tx;
		ArrayList<String> templines = new ArrayList<String>();
		HashMap<String,ArrayList<ArrayList<String>>> synonymhash = null;
		boolean synFileExists = false;
		if(synonymfile.length()>0)
			synFileExists = true;
		//preprocess the synonym file
		//key is the id from the taxonomy, the array has the synonym and the type of synonym
		if(synFileExists){
			synonymhash = new HashMap<String,ArrayList<ArrayList<String>>>();
			try {
				BufferedReader sbr = new BufferedReader(new FileReader(synonymfile));
				while((str = sbr.readLine())!=null){
					StringTokenizer st = new StringTokenizer(str,"\t|\t");
					String id = st.nextToken();
					String name = st.nextToken();
					String type = st.nextToken();
					ArrayList<String> tar = new ArrayList<String>();
					tar.add(name);tar.add(type);
					if (synonymhash.get(id) == null){
						ArrayList<ArrayList<String> > ttar = new ArrayList<ArrayList<String> >();
						synonymhash.put(id, ttar);
					}
					synonymhash.get(id).add(tar);
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}
			System.out.println("synonyms: "+synonymhash.size());
		}
		//finished processing synonym file
		HashMap<String, Node> dbnodes = new HashMap<String, Node>();
		HashMap<String, String> parents = new HashMap<String, String>();
		try{
			tx = graphDb.beginTx();
			//create the metadata node
			Node metadatanode = null;
			try{
				metadatanode = graphDb.createNode();
				metadatanode.setProperty("source", sourcename);
				metadatanode.setProperty("author", "no one");
				taxSourceIndex.add(metadatanode, "source", sourcename);
				tx.success();
			}finally{
				tx.finish();
			}
			BufferedReader br = new BufferedReader(new FileReader(filename));
			while((str = br.readLine())!=null){
				count += 1;
				templines.add(str);
				if (count % transaction_iter == 0){
					System.out.print(count);
					System.out.print("\n");
					tx = graphDb.beginTx();
					try{
						for(int i=0;i<templines.size();i++){
							StringTokenizer st = new StringTokenizer(templines.get(i),"\t|\t");
							int numtok = st.countTokens();
							String first = st.nextToken();
							String second = "";
							if(numtok == 3)
								second = st.nextToken();
							String third = st.nextToken();
							Node tnode = graphDb.createNode();
							tnode.setProperty("name", third);
							taxNodeIndex.add( tnode, "name", third);
							dbnodes.put(first, tnode);
							if (numtok == 3){
								parents.put(first, second);
							}else{//this is the root node
								System.out.println("created root node and metadata link");
								metadatanode.createRelationshipTo(tnode, RelTypes.METADATAFOR);
							}
							//synonym processing
							if(synFileExists){
								if(synonymhash.get(first)!=null){
									ArrayList<ArrayList<String>> syns = synonymhash.get(first);
									for(int j=0;j<syns.size();j++){
										Node synode = graphDb.createNode();
										synode.setProperty("name",syns.get(j).get(0));
										synode.setProperty("nametype",syns.get(j).get(1));
										synode.setProperty("source",sourcename);
										synode.createRelationshipTo(tnode, RelTypes.SYNONYMOF);
									}
								}
							}
						}
						tx.success();
					}finally{
						tx.finish();
					}
					templines.clear();
				}
			}
			br.close();
			tx = graphDb.beginTx();
			try{
				for(int i=0;i<templines.size();i++){
					StringTokenizer st = new StringTokenizer(templines.get(i),"\t|\t");
					int numtok = st.countTokens();
					String first = st.nextToken();
					String second = "";
					if(numtok == 3)
						second = st.nextToken();
					String third = st.nextToken();
					Node tnode = graphDb.createNode();
					tnode.setProperty("name", third);
					taxNodeIndex.add( tnode, "name", third);
					dbnodes.put(first, tnode);
					if (numtok == 3){
						parents.put(first, second);
					}else{//this is the root node
						System.out.println("created root node and metadata link");
						metadatanode.createRelationshipTo(tnode, RelTypes.METADATAFOR);
					}
					//synonym processing
					if(synFileExists){
						if(synonymhash.get(first)!=null){
							ArrayList<ArrayList<String>> syns = synonymhash.get(first);
							for(int j=0;j<syns.size();j++){
								Node synode = graphDb.createNode();
								synode.setProperty("name",syns.get(j).get(0));
								synode.setProperty("nametype",syns.get(j).get(1));
								synode.setProperty("source",sourcename);
								synode.createRelationshipTo(tnode, RelTypes.SYNONYMOF);
							}
						}
					}
				}
				tx.success();
			}finally{
				tx.finish();
			}
			templines.clear();
			//add the relationships
			ArrayList<String> temppar = new ArrayList<String>();
			count = 0;
			for(String key: dbnodes.keySet()){
				count += 1;
				temppar.add(key);
				if (count % transaction_iter == 0){
					System.out.println(count);
					tx = graphDb.beginTx();
					try{
						for (int i=0;i<temppar.size();i++){
							try {
								Relationship rel = dbnodes.get(temppar.get(i)).createRelationshipTo(dbnodes.get(parents.get(temppar.get(i))), RelTypes.TAXCHILDOF);
								rel.setProperty("source", sourcename);
								rel.setProperty("childid",temppar.get(i));
								rel.setProperty("parentid",parents.get(temppar.get(i)));
							}catch(java.lang.IllegalArgumentException io){
//								System.out.println(temppar.get(i));
								continue;
							}
						}
						tx.success();
					}finally{
						tx.finish();
					}
					temppar.clear();
				}
			}
			tx = graphDb.beginTx();
			try{
				for (int i=0;i<temppar.size();i++){
					try {
						Relationship rel = dbnodes.get(temppar.get(i)).createRelationshipTo(dbnodes.get(parents.get(temppar.get(i))), RelTypes.TAXCHILDOF);
						rel.setProperty("source", sourcename);
						rel.setProperty("childid",temppar.get(i));
						rel.setProperty("parentid",parents.get(temppar.get(i)));
					}catch(java.lang.IllegalArgumentException io){
//						System.out.println(temppar.get(i));
						continue;
					}
				}
				tx.success();
			}finally{
				tx.finish();
			}
		}catch(IOException ioe){}
	}
	
	/**
	 * Returns a pair of integers that reflect the indices of element in the lists
	 * 	that match (lowest index of an element in keylist, and its match in
	 *	list1). 
	 * 
	 * @param keylist first array of strings to search
	 * @param list1 second array of strings to search
	 * @return pair of ints [i, j] where i is 1 + the index of the first 
	 *		element in keylist that has a match in list1, and j is 1 + the
	 *		lowest index for any element in list1 that matches the element in 
	 *		keylist. Returns [LARGE, LARGE] if no matching strings are found.
	 */
	private ArrayList<Integer> stepsToMatch(ArrayList<String> keylist, ArrayList<String> list1){
		int count1 = 0;
		ArrayList<Integer> ret = new ArrayList<Integer>();
		for (int i=0;i < keylist.size(); i++){
			count1 += 1;
			int count2 = 0;
			for (int j=0; j< list1.size();j++){
				count2 += 1;
				if (list1.get(j).compareTo(keylist.get(i)) == 0){
					ret.add(count1);ret.add(count2);
					return ret;
				}
			}
		}
		ret.add(LARGE);ret.add(LARGE);
		return ret;
	}
	
	/**
	 * Helper function used by addAdditionalTaxonomyTableIntoGraph
	 * adds the nodes as new Node objects into this.graphDb
	 * addnodes and addnodesids are matched lists of the node names and node ids
	 *
	 * @param addnodes list of names to added
	 * @param addnodesids list of ids associated with the nodes to added
	 * @param addednodes map of ids to the newly created node objects
	 */
	private void addBatchOfNewNodes(ArrayList<String> name_list, ArrayList<String> id_list, HashMap<String,Node> addednodes) {
		assert name_list.size() == id_list.size();
		Transaction tx;
		tx = graphDb.beginTx();
		try{
			for(int i = 0; i < name_list.size(); i++){
				Node tnode = graphDb.createNode();
				tnode.setProperty("name", name_list.get(i));
				taxNodeIndex.add( tnode, "name", name_list.get(i));
				_LOG.debug("Added " + name_list.get(i));
				addednodes.put(id_list.get(i), tnode);
			}
			tx.success();
		}finally{
			tx.finish();
		}
	}
	
	private String taxonPathAsString(ArrayList<String> path) {
		StringBuffer sb = new StringBuffer();
		boolean first = true;
		for (String tax_name : path ) {
			if (first) {
				first = false;
			}
			else {
				sb.append(" -> ");
			}
			sb.append(tax_name);
		}
		return sb.toString();
	}
	
	/**
	 * The idea for this is to use a preorder traversal to insert a new taxonomy
	 * into the graph (along with potentially new synonyms)
	 * 
	 * @param sourcename
	 * @param rootid
	 * @param filename
	 * @param synonymfile
	 */
	public void addAdditionalTaxonomyToGraphNEW(String sourcename, String rootid, String filename, String synonymfile){
		Node rootnode = null;
		String roottaxid = "";
		if (rootid.length() > 0){
			rootnode = graphDb.getNodeById(Long.valueOf(rootid));
			System.out.println(rootnode);
		}
		String str = "";
		int count = 0;
		Transaction tx;
		ArrayList<String> templines = new ArrayList<String>();
		HashMap<String,ArrayList<ArrayList<String>>> synonymhash = null;
		boolean synFileExists = false;
		if(synonymfile.length()>0)
			synFileExists = true;
		//preprocess the synonym file
		//key is the id from the taxonomy, the array has the synonym and the type of synonym
		if(synFileExists){
			synonymhash = new HashMap<String,ArrayList<ArrayList<String>>>();
			try {
				BufferedReader sbr = new BufferedReader(new FileReader(synonymfile));
				while((str = sbr.readLine())!=null){
					StringTokenizer st = new StringTokenizer(str,"\t|\t");
					String id = st.nextToken();
					String name = st.nextToken();
					String type = st.nextToken();
					ArrayList<String> tar = new ArrayList<String>();
					tar.add(name);tar.add(type);
					if (synonymhash.get(id) == null){
						ArrayList<ArrayList<String> > ttar = new ArrayList<ArrayList<String> >();
						synonymhash.put(id, ttar);
					}
					synonymhash.get(id).add(tar);
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}
			System.out.println("synonyms: "+synonymhash.size());
		}
		//finished processing synonym file
		HashMap<String, String> parents = new HashMap<String, String>();
		HashMap<String, String> idnamemap = new HashMap<String, String>();
		HashMap<String,ArrayList<String>> children = new HashMap<String,ArrayList<String>>();
		tx = graphDb.beginTx();
		try{
			//create the metadata node
			Node metadatanode = null;
			try{
				metadatanode = graphDb.createNode();
				metadatanode.setProperty("source", sourcename);
				metadatanode.setProperty("author", "no one");
				taxSourceIndex.add(metadatanode, "source", sourcename);
				tx.success();
			}finally{
				tx.finish();
			}
			tx = graphDb.beginTx();
			try{
				BufferedReader br = new BufferedReader(new FileReader(filename));
				while((str = br.readLine())!=null){
					count += 1;
					if (count % transaction_iter == 0){
						System.out.print(count);
						System.out.print("\n");
					}
					StringTokenizer st = new StringTokenizer(str,"\t|\t");
					int numtok = st.countTokens();
					String first = st.nextToken();
					String second = "";
					if(numtok == 3)
						second = st.nextToken();
					String third = st.nextToken();
					idnamemap.put(first, third);
					if (numtok == 3){
						parents.put(first, second);
						if(children.containsKey(second) == false){
							ArrayList<String> tar = new ArrayList<String>();
							children.put(second, tar);
						}
						children.get(second).add(first);
					}else{//this is the root node
						if(rootnode == null){
							System.out.println("the root should never be null");
							System.exit(0);
							//if the root node is null then you need to make a new one
							//rootnode = graphDb.createNode();
							//rootnode.setProperty("name", third);
						}
						roottaxid = first;
						System.out.println("matched root node and metadata link");
						metadatanode.createRelationshipTo(rootnode, RelTypes.METADATAFOR);
					}
				}
				br.close();
			}catch(Exception e){
				e.printStackTrace();
				System.out.println("problem with infile");
				System.exit(0);
			}
			tx.success();
		}finally{
			tx.finish();
		}
		//now start the preorder after the processing of the file
		globaltranscationnum = 0;
		gtx = graphDb.beginTx();
		try{
			System.out.println("sending to preorder builder");
			globalchildren = children;
			globalidnamemap = idnamemap;
			preorderAddAdditionalTaxonomy(rootnode,rootnode,roottaxid,sourcename);
			preorderFinishTransaction();
			gtx.success();
		}finally{
			gtx.finish();
		}
	}

	HashMap<String, ArrayList<String>> globalchildren = null;
	HashMap<String,String> globalidnamemap = null;
	PathFinder<Path> finder = GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(RelTypes.TAXCHILDOF, Direction.OUTGOING ),10000);
	HashMap<Node,Node> lastexistingmatchparents = new HashMap<Node,Node>();
	
	private void preorderFinishTransaction(){
		try{
			gtx.success();
		}finally{
			gtx.finish();
		}
		gtx = graphDb.beginTx();		
	}
	
	private void preorderAddAdditionalTaxonomy(Node lastexistingmatch, Node rootnode, String curtaxid,String sourcename) {
		globaltranscationnum += 1;
		//using the current node, root node, see if the children have any matches, if they do
		//then they much be subtending of the current rootnode
		if(globaltranscationnum % (transaction_iter/100) == 0){
			System.out.println("preorder add: "+globaltranscationnum);
			preorderFinishTransaction();
		}
		boolean verbose = false;
		//this is the preorder part
		ArrayList<String> childids = globalchildren.get(curtaxid);
		if(childids != null){
			for(int i=0;i<childids.size();i++){
				/*if(globalidnamemap.get(childids.get(i)).equals("Lonicera")){
					verbose = true;
				}*/
				//create nodes and relationships here
				Node hitnode = null;
				IndexHits<Node> hits = taxNodeIndex.get("name", globalidnamemap.get(childids.get(i)));
				
				try{
					if(verbose){
						System.out.println("hits: "+hits.size());
					}
					for(Node nd : hits){
						//check to see if there is a path from the lastexistingmatch and the hit node
						//if there is a hit, you take the closest and report that there was ambiguity
						Node curnode = lastexistingmatch;
						Path pathit = null;
						boolean going = true;
						while(going){
							pathit = finder.findSinglePath(nd, curnode);
							if(verbose){
								System.out.println("node: "+nd+"("+nd.getProperty("name")+") last: "+lastexistingmatch+"("+curnode.getProperty("name")+")");
								System.out.println("path: "+pathit);
							}	
							if (pathit != null){	
								//should add the smaller distance
								break;
							}
							if (lastexistingmatchparents.containsKey(curnode) == true)
								curnode = lastexistingmatchparents.get(curnode);
							else
								going = false;
						}
						if (pathit != null){	
							if(verbose){
								System.out.println("MATCHED");
								System.out.println("node: "+nd+"("+nd.getProperty("name")+") last: "+lastexistingmatch+"("+lastexistingmatch.getProperty("name")+")");
								System.out.println("path: "+pathit);
							}	
							//should add the smaller distance
							hitnode = nd;
							lastexistingmatchparents.put(hitnode, lastexistingmatch);
							lastexistingmatch = hitnode;
							break;
						}
					}
					if(verbose){
						System.out.println("hit: "+hitnode);
					}
					//if there was no hit, need to create a node
					if(hitnode == null){
						hitnode = graphDb.createNode();
						hitnode.setProperty("name", globalidnamemap.get(childids.get(i)));
						taxNodeIndex.add(hitnode, "name", globalidnamemap.get(childids.get(i)));
					}
					Relationship rel = hitnode.createRelationshipTo(rootnode, RelTypes.TAXCHILDOF);
					rel.setProperty("source", sourcename);
				}finally{
					hits.close();
				}
				if(verbose){
					verbose = false;
				}
				preorderAddAdditionalTaxonomy(lastexistingmatch,hitnode,childids.get(i),sourcename);
			}
		}
	}
	

	/**
	 * See addInitialTaxonomyTableIntoGraph 
	 * This function acts like addInitialTaxonomyTableIntoGraph but it 
	 *	can be called for a taxonomy that is not the first taxonomy in the graph
	 * 
	 * Rather than each line resulting in a new node, only names that have not
	 *		 been encountered before will result in new node objects.
	 *
	 * To connect a subtree from the new taxonomy to the taxonomy tree the 
	 *	taxNodeIndex of the existing graph is checked the new name. If multiple
	 *	nodes have been assigned the name, then the one with the lowest score
	 *	is assumed to be the closest match (the score is calculated by counting
	 *	the number of nodes traversed in the path new->anc* + the number of 
	 *	nodes in old->anc* where "anc*" denotes the lowest ancestor in
	 *	the new taxon's ancestor path that has a match in the old graph (and
	 *	the TAXCHILDOF is the relationship on the path).
	 *	
	 * @param filename file path to the taxonomy file
	 * @param sourcename this becomes the value of a "source" property in every relationship between the taxonomy nodes
	 */
	public void addAdditionalTaxonomyToGraph(String sourcename, String filename, String synonymfile){
		String str = "";
		int count = 0;
		HashMap<String, String> ndnames = new HashMap<String, String>(); // node number -> name
		HashMap<String, String> parents = new HashMap<String, String>(); // node number -> parent's number
		Transaction tx;
		ArrayList<String> addnodes = new ArrayList<String>();
		ArrayList<String> addnodesids = new ArrayList<String>();
		HashMap<String,Node> addednodes = new HashMap<String,Node>();
		//first, need to get what nodes are new
		try{
			BufferedReader br = new BufferedReader(new FileReader(filename));
			while((str = br.readLine())!=null){
				count += 1;
				String[] spls = str.split(",");
				parents.put(spls[0], spls[1]);
				String strname = spls[2];
				ndnames.put(spls[0], strname);
				IndexHits<Node> ih = taxNodeIndex.get("name", strname);
				try{
					if(ih.size() == 0){
						addnodes.add(strname);
						addnodesids.add(spls[0]);
					}
//						else {
//						_LOG.trace(strname + " already in db");
//					}
				}finally{
					ih.close();
				}
				if (count % transaction_iter == 0){
					System.out.print(count);
					System.out.print(" ");
					System.out.print(addnodes.size());
					System.out.print("\n");
					addBatchOfNewNodes(addnodes, addnodesids, addednodes);
					addnodes.clear();
					addnodesids.clear();
				}
			}
			br.close();
		}catch(IOException ioe){}
		addBatchOfNewNodes(addnodes, addnodesids, addednodes);
		addnodes.clear();
		addnodesids.clear();
		
		System.out.println("second pass through file for relationships");
		//GET NODE
		ArrayList<Node> rel_nd = new ArrayList<Node>();
		ArrayList<Node> rel_pnd = new ArrayList<Node>();
		ArrayList<String> rel_cid = new ArrayList<String>();
		ArrayList<String> rel_pid = new ArrayList<String>();
		try{
			count = 0;
			BufferedReader br = new BufferedReader(new FileReader(filename));
			while((str = br.readLine())!=null){
				String[] spls = str.split(",");
				count += 1;
				String nameid = spls[0];
				String strparentid = spls[1];
				String strname = spls[2];
				String strparentname = "";
				if(spls[1].compareTo("0") != 0)
					strparentname = ndnames.get(spls[1]);
				else
					continue;
//				_LOG.trace(str);
				
				//get full path to the root of the input taxonomy
				// path1 will contain the node -> root list of all names
				// badpath will be true if there is no parent returned for a node along this path
				ArrayList<String> path1 = new ArrayList<String>();
				boolean badpath = false;
				String cur = parents.get(spls[0]);
//				_LOG.trace("parent:"+cur +" "+ndnames.get(cur));
				for(;;){
					if(cur == null){
						badpath = true;
//						_LOG.warn("-bad path start:"+spls[0]);
						break;
					}else if (cur.compareTo("0") == 0){
						break;
					}else{
//						_LOG.trace("parent:"+cur +" "+ndnames.get(cur));
						path1.add(ndnames.get(cur));
						cur = parents.get(cur);
					}
				}
				
				/*
				 * if the nodes that don't lead to the root don't have
				 * other relationships, delete the nodes
				 * TODO: test this!
				 */
				if(badpath == true){
					IndexHits<Node> hits = taxNodeIndex.get("name", strname);
					try{
						for(Node nd : hits){
							if(nd.hasRelationship()==false){
								tx = graphDb.beginTx();
								try{
									nd.delete();
									tx.success();
								}finally{
									tx.finish();
								}
							}
						}
					}finally{
						hits.close();
					}
					continue;
				}
				
				Node matchnode = null;
				int bestcount = LARGE;
				Node bestitem = null;
				ArrayList<String> bestpath = null;
				ArrayList<Node> bestpathitems= null;
				IndexHits<Node> hits = taxNodeIndex.get("name", strname);
				ArrayList<String> path2 = null;
				ArrayList<Node> path2items = null;
				/*
				 * get the best hit by walking the parents
				 */
				if(addednodes.containsKey((String)nameid) == false){//name was not added this time around
					try{
						for(Node node: hits){
							path2 = new ArrayList<String> ();
							path2items = new ArrayList<Node> ();
							//get shortest path
							for(Node currentNode : CHILDOF_TRAVERSAL.traverse(node).nodes()){
//								_LOG.trace("+"+((String) currentNode.getProperty("name")));
								if(((String) currentNode.getProperty("name")).compareTo(strname) != 0){
									path2.add((String)currentNode.getProperty("name"));
									path2items.add(currentNode);
//									_LOG.trace((String)currentNode.getProperty("name"));
								}
							}
							ArrayList<Integer> itemcounts = stepsToMatch(path1,path2);
							//if(GeneralUtils.sum_ints(itemcounts.get(node)) < bestcount || first == true){
							if(itemcounts.get(0) < bestcount){
								bestcount = itemcounts.get(0);
								bestitem = node;
								bestpath = new ArrayList<String>(path2);
								bestpathitems = new ArrayList<Node>(path2items);
							}
							path2.clear();
							path2items.clear();
//							_LOG.trace(bestcount);
							//_LOG.trace("after:"+bestpath.get(1));
						}
					}finally{
						hits.close();
					}
					//if the match is worse than the threshold, make a new node
					if (bestitem == null){
						System.out.println("adding duplicate "+strname);
						System.out.println(path1);
						hits = taxNodeIndex.get("name", strname);
						for(Node node: hits){
							path2 = new ArrayList<String> ();
							path2items = new ArrayList<Node> ();
							System.out.println("node: " +node.getProperty("name"));
							//get shortest path
							for(Node currentNode : CHILDOF_TRAVERSAL.traverse(node).nodes()){
								System.out.println("currentnode: "+currentNode.getProperty("name"));
								if(((String) currentNode.getProperty("name")).compareTo(strname) != 0){
									path2.add((String)currentNode.getProperty("name"));
									path2items.add(currentNode);
								}
							}
							ArrayList<Integer> itemcounts = stepsToMatch(path1,path2);
							System.out.println(itemcounts);
							System.out.println(path2);
						}
						hits.close();
						_LOG.warn("adding duplicate " + strname);
//						if(_LOG.isDebugEnabled()) {
//							_LOG.debug("path1:    " + strname + " -> " + this.taxonPathAsString(path1));
//							_LOG.debug("bestpath: " + strname + " -> " + this.taxonPathAsString(bestpath));
//							_LOG.debug("bestcount = " + bestcount + " path1.size() - bestcount =" + (path1.size() - bestcount));
//						}
						tx = graphDb.beginTx();
						try{
							Node tnode = graphDb.createNode();
							tnode.setProperty("name", strname);
							taxNodeIndex.add( tnode, "name", strname);
							bestitem = tnode;
							bestpath = new ArrayList<String>();
							bestpathitems = new ArrayList<Node>();
							tx.success();
							addednodes.put(nameid, tnode);
						}finally{
							tx.finish();
						}
					}
				}//name was added this time around
				else{
					bestitem = addednodes.get(nameid);
					bestpath = new ArrayList<String>();
					bestpathitems = new ArrayList<Node>();
				}
				
				matchnode = bestitem;
				if(spls[1].compareTo("0") != 0){
					Node matchnodeparent = null;
					for(int i=0;i<bestpath.size();i++){
						if(bestpath.get(i).compareTo(strparentname) == 0){
							matchnodeparent = bestpathitems.get(i);
//							break;
						}
//						_LOG.trace("="+bestpath.get(i)+" "+strparentname);
					}
					//do the same as above with the parent
					if(matchnodeparent == null && addednodes.containsKey(strparentid) == false){
						path1.remove(0);
						bestcount = LARGE;
						bestitem = null;
						hits = taxNodeIndex.get("name", strparentname);
						if(strparentname.compareTo("life")==0){//special case for the life node in the graph
							if (hits.size()>1){
								System.out.println("too many life nodes. what's the deal");
							}
							bestitem = hits.getSingle();
							path2 = null;
							path2items = null;
						}else{
							path2 = null;
							path2items = null;
							/*
							 * get the best hit by walking the parents
							 */
							try{
								for(Node node: hits){
									path2 = new ArrayList<String> ();
									path2items = new ArrayList<Node> ();
									//get shortest path
									for(Node currentNode : CHILDOF_TRAVERSAL.traverse(node).nodes()){
										if(((String) currentNode.getProperty("name")).compareTo(strparentname) != 0){
											path2.add((String)currentNode.getProperty("name"));
	//										_LOG.trace("path2: "+(String)currentNode.getProperty("name"));
											path2items.add(currentNode);
										}
									}
									ArrayList<Integer> itemcounts = stepsToMatch(path1,path2);
	//								_LOG.trace(itemcounts.get(0));
									if(itemcounts.get(0) < bestcount){
										bestcount = itemcounts.get(0);
										bestitem = node;
									}
									path2.clear();
									path2items.clear();
	//								_LOG.trace(bestcount);
								}
							}finally{
								hits.close();
							}
						}
						//if the match is worse than the threshold, make a new node
//						_LOG.trace(path1.size()+" "+(path1.size()-bestcount));
						if (bestitem == null){
							System.out.println("adding duplicate parent "+strparentname);
//							if(_LOG.isDebugEnabled()) {
//								_LOG.debug("path1:    " + strname + " -> " + this.taxonPathAsString(path1));
//								_LOG.debug("bestcount = " + bestcount + " path1.size() - bestcount =" + (path1.size() - bestcount));
//							}
							tx = graphDb.beginTx();
							try{
								Node tnode = graphDb.createNode();
								tnode.setProperty("name", strparentname);
								taxNodeIndex.add( tnode, "name", strparentname);
								bestitem = tnode;
								tx.success();
								addednodes.put(strparentid, tnode);
							}finally{
								tx.finish();
							}
						}
						matchnodeparent = bestitem;
					}else if (addednodes.containsKey(strparentid) == true){
						matchnodeparent = addednodes.get(strparentid);
					}
					rel_nd.add(matchnode);
					rel_pnd.add(matchnodeparent);
					rel_cid.add(spls[0]);
					rel_pid.add(spls[1]);
				}

				if(count % transaction_iter == 0){
					System.out.println(count);
					tx = graphDb.beginTx();
					try{
						for(int i=0;i<rel_nd.size();i++){
							try{
							Relationship rel = rel_nd.get(i).createRelationshipTo(rel_pnd.get(i), RelTypes.TAXCHILDOF);
							rel.setProperty("source", sourcename);
							rel.setProperty("childid", rel_cid.get(i));
							rel.setProperty("parentid", rel_pid.get(i));
							}catch(java.lang.Exception jle){
								System.out.println(rel_cid.get(i) +" "+rel_pid.get(i));
								System.out.println(rel_nd.get(i).getProperty("name")+" "+rel_pnd.get(i).getProperty("name"));
								System.exit(0);
							}
						}
						rel_nd.clear();
						rel_pnd.clear();
						rel_cid.clear();
						rel_pid.clear();
						tx.success();
					}finally{
						tx.finish();
					}
				}
				path1.clear();
			}
			tx = graphDb.beginTx();
			try{
				for(int i=0;i<rel_nd.size();i++){
					Relationship rel = rel_nd.get(i).createRelationshipTo(rel_pnd.get(i), RelTypes.TAXCHILDOF);
					rel.setProperty("source", sourcename);
					rel.setProperty("childid", rel_cid.get(i));
					rel.setProperty("parentid", rel_pid.get(i));
				}
				rel_nd.clear();
				rel_pnd.clear();
				rel_cid.clear();
				rel_pid.clear();
				tx.success();
			}finally{
				tx.finish();
			}
			br.close();
		}catch(IOException ioe){}
	}
	
	public void runittest(String filename,String filename2){

	}
	
	public static void main( String[] args ){
		System.out.println( "unit testing taxonomy loader" );
		String DB_PATH ="/home/smitty/Dropbox/projects/AVATOL/graphtests/neo4j-community-1.8.M02/data/graph.db";
		TaxonomyLoader a = new TaxonomyLoader(DB_PATH);
		//String filename = "/media/data/Dropbox/projects/AVATOL/graphtests/taxonomies/union4.txt";
		String filename =  "/home/smitty/Dropbox/projects/AVATOL/graphtests/taxonomies/col_acc.txt";
		String filename2 = "/home/smitty/Dropbox/projects/AVATOL/graphtests/taxonomies/ncbi_no_env_samples.txt";
		a.runittest(filename,filename2);
		System.exit(0);
	}
}
