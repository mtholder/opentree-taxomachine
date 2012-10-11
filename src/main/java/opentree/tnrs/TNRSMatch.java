package opentree.tnrs;
//import java.lang.Long;
import java.util.Map;
import java.util.HashMap;

import org.neo4j.graphdb.Node;

/**
 * A TNRSMatch object represents a validated hit for some query string to a recognized name or synonym in some source, and as such they
 * contain relevant metadata for the represented match. TNRSMatch objects are returned by various methods of the TNRSMatchSet objects
 * that contain them, and are immutable, as they represent external database records that cannot be changed except by external means.
 * @author Cody Hinchliff
 *
 */
public abstract class TNRSMatch {
    public abstract String getSearchString();
    public abstract long getMatchedNodeId();
    public abstract String getMatchedNodeName();
    public abstract Node getMatchedNode();    
    public abstract long getSynonymNodeId();
    public abstract String getSynonymNodeName();
    public abstract String getSource();
    public abstract boolean getIsExactNode();
    public abstract boolean getIsApproximate();
    public abstract boolean getIsSynonym();
    public abstract boolean getIsHomonym();
    public abstract double getScore();
    public abstract String getMatchType();
    public abstract String toString();
    public Map<String, Object> getAttributes() {
        Map<String, Object> hm = new HashMap<String, Object>();
        hm.put("matched_node_id", new Long(this.getMatchedNodeId()));
        hm.put("matched_node_name", this.getMatchedNodeName());
//                response += "\"synonym_node_id\":" + m.getSynonymNodeId() + "\",";
//                response += "\"synonym_node_name\":\"" + m.getSynonymNodeName() + "\","; 
        hm.put("source", this.getSource());
        hm.put("is_exact_node", new Boolean(this.getIsExactNode()));
        hm.put("is_approximate_node", new Boolean(this.getIsApproximate()));
        hm.put("is_synonym", new Boolean(this.getIsSynonym()));
        hm.put("is_homonym", new Boolean(this.getIsHomonym()));
        hm.put("score", new Double(this.getScore()));
        return hm;
    }
    
}
