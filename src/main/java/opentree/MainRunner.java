package opentree;

//import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class MainRunner {
	public void taxonomyLoadParser(String [] args) {
		String graphname = "";
		String synonymfile = "";
		if (args[0].equals("inittax") || args[0].equals("addtax")) {
			if (args.length != 4) {
				System.out.println("arguments should be: sourcename filename graphdbfolder");
				return;
			} else {
				graphname = args[3];
			}
		} else if (args[0].equals("inittaxsyn") || args[0].equals("addtaxsyn")) {
			if (args.length != 5) {
				System.out.println("arguments should be: sourcename filename synonymfile graphdbfolder");
				return;
			} else {
				synonymfile = args[3];
				graphname = args[4];
			}
		}
		String sourcename = args[1];
		String filename = args[2];
		TaxonomyLoader tl = new TaxonomyLoader(graphname);
		if (args[0].equals("inittax")) {
			System.out.println("initializing taxonomy from " + filename + " to " + graphname);
			tl.initializeTaxonomyIntoGraph(sourcename,filename,synonymfile);
		} else if(args[0].equals("addtax")) {
			System.out.println("adding taxonomy from " + filename + " to "+ graphname);
			tl.addAdditionalTaxonomyToGraphNEW(sourcename, "1300014", filename, synonymfile); // '1300014' = root ID
		} else if (args[0].equals("inittaxsyn")) {
			System.out.println("initializing taxonomy from " + filename + " and synonym file " + synonymfile + " to " + graphname);
			tl.initializeTaxonomyIntoGraph(sourcename,filename,synonymfile);
		} else if (args[0].equals("addtaxsyn")) {
			System.out.println("adding taxonomy from " + filename + "and synonym file " + synonymfile + " to " + graphname);
			tl.addAdditionalTaxonomyToGraphNEW(sourcename, "1300014", filename,synonymfile);
		} else {
			System.err.println("\nERROR: not a known command");
			tl.shutdownDB();
			printHelp();
			System.exit(1);
		}
		tl.shutdownDB();
	}
	
	
	public void taxonomyQueryParser(String [] args) {
		if (args[0].equals("checktree")) {
			if (args.length != 4) {
				System.out.println("arguments should be: treefile focalgroup graphdbfolder");
				return;
			}
		} else if (args[0].equals("comptaxgraph")) {
			if (args.length != 4) {
				System.out.println("arguments should be: comptaxgraph query graphdbfolder outfile");
				return;
			}
		} else if (args[0].equals("makeottol")) {
			if (args.length != 2) {
				System.out.println("arguments should be: graphdbfolder");
				return;
			}
		} else if (args.length != 3) {
			System.out.println("arguments should be: query graphdbfolder");
			return;
		}
		
		TaxonomyExplorer te = null;
		if (args[0].equals("comptaxtree")) {
			String query = args[1];
			String graphname = args[2];
			te =  new TaxonomyExplorer(graphname);
			System.out.println("constructing a comprehensive tax tree of " + query);
			te.buildTaxonomyTree(query);
		} else if (args[0].equals("comptaxgraph")) {
			String query = args[1];
			String graphname = args[2];
			String outname = args[3];
			te =  new TaxonomyExplorer(graphname);
			te.exportGraphForClade(query, outname);
		} else if (args[0].equals("findcycles")) {
			String query = args[1];
			String graphname = args[2];
			te =  new TaxonomyExplorer(graphname);
			System.out.println("finding taxonomic cycles for " + query);
			te.findTaxonomyCycles(query);
		} else if (args[0].equals("jsgraph")) {
			String query = args[1];
			String graphname = args[2];
			te =  new TaxonomyExplorer(graphname);
			System.out.println("constructing json graph data for " + query);
			te.constructJSONGraph(query);
		} else if (args[0].equals("checktree")) {
			String query = args[1];
			String focalgroup = args[2];
			String graphname = args[3];
			te =  new TaxonomyExplorer(graphname);
			System.out.println("checking the names of " + query + " against the taxonomy graph");
			te.checkNamesInTree(query,focalgroup);
		} else if (args[0].equals("makeottol")) {
			String graphname = args[1];
			te =  new TaxonomyExplorer(graphname);
			System.out.println("making ottol relationships");
			te.makePreferredOTTOLRelationshipsConflicts();
			te.makePreferredOTTOLRelationshipsNOConflicts();
		} else {
			System.err.println("\nERROR: not a known command\n");
			//te.shutdownDB(); // can only be null here
			printHelp();
			System.exit(1);
		}
		te.shutdownDB();
	}

	
	public static void printHelp(){
		System.out.println("==========================");
		System.out.println("usage: taxomachine command options");
		System.out.println("");
		System.out.println("commands");
		System.out.println("---taxonomy---");
		System.out.println("\tinittax <sourcename> <filename> <graphdbfolder> (initializes the tax graph with a tax list)");
		System.out.println("\taddtax <sourcename> <filename> <graphdbfolder> (adds a tax list into the tax graph)");
		System.out.println("\tinittaxsyn <sourcename> <filename> <synonymfile> <graphdbfolder> (initializes the tax graph with a list and synonym file)");
		System.out.println("\taddtaxsyn <sourcename> <filename> <synonymfile> <graphdbfolder> (adds a tax list and synonym file)");
		System.out.println("\tupdatetax <filename> <sourcename> <graphdbfolder> (updates a specific source taxonomy)");
		System.out.println("\tmakeottol <graphdbfolder> (creates the preferred ottol branches)");
		System.out.println("\n---taxquery---");
		System.out.println("\tcomptaxtree <name> <graphdbfolder> (construct a comprehensive tax newick)");
		System.out.println("\tcomptaxgraph <name> <graphdbfolder> <outdotfile> (construct a comprehensive taxonomy in dot)");
		System.out.println("\tfindcycles <name> <graphdbfolder> (find cycles in tax graph)");
		System.out.println("\tjsgraph <name> <graphdbfolder> (constructs a json file from tax graph)");
		System.out.println("\tchecktree <filename> <focalgroup> <graphdbfolder> (checks names in tree against tax graph)");
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		PropertyConfigurator.configure(System.getProperties());
		System.out.println("taxomachine version alpha.alpha.prealpha");
		
		if (args.length == 0 || args[0].equals("help")) {
			printHelp();
			System.exit(0);
		} else if (args.length < 2) {
			System.err.println("\nERROR: expecting multiple arguments\n");
			printHelp();
			System.exit(1);
		} else {
			System.out.println("\nThings will happen here!\n");
			MainRunner mr = new MainRunner();
			
			if (args[0].equals("inittax")
					|| args[0].equals("addtax")
					|| args[0].equals("inittaxsyn")
					|| args[0].equals("addtaxsyn")) {
				mr.taxonomyLoadParser(args);
			} else if (args[0].equals("comptaxtree")
					|| args[0].equals("comptaxgraph")
					|| args[0].equals("findcycles")
					|| args[0].equals("jsgraph") 
					|| args[0].equals("checktree")
					|| args[0].equals("makeottol")) {
				mr.taxonomyQueryParser(args);
			} else {
				System.err.println("\nERROR: unrecognized command \"" + args[0] + "\"\n");
				printHelp();
				System.exit(1);
			}
		}
	}
}
