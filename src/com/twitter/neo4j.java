package com.twitter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

//import org.apache.log4j.Logger;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
//import org.neo4j.graphdb.index.Index;
//import org.neo4j.graphdb.index.IndexHits;
//import org.neo4j.graphdb.index.IndexManager;
//import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.graphdb.index.UniqueFactory;

//import com.neo.HelloNeo4J.RelTypes;

//import com.twitter.HelloNeo4J.RelTypes;

//import com.twitter.neo4j.RelTypes;

@SuppressWarnings("deprecation")
public class neo4j {
	private static final String DB_PATH = "C:/workspace/Twitter_final/neo4j";

    static String myString;
    static GraphDatabaseService graphDb;// = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
   // static Node myFirstNode;
    //static Node mySecondNode;
   // static Relationship myRelationship;

    private static enum RelTypes implements RelationshipType
    {
        PARENT,SON
    }
    
    @SuppressWarnings("static-access")
	static
	void createDb()
    {
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );       
    }
    
    @SuppressWarnings("static-access")
	static
	void shutDown()
    {
        graphDb.shutdown();
        //System.out.println("graphDB shut down.");   
    }
    
    public static Node getOrCreateNodeWithUniqueFactory( String search_term, GraphDatabaseService graphDb )
	 {
    	//createDb();
	     UniqueFactory<Node> factory = new UniqueFactory.UniqueNodeFactory( graphDb, "search_term" )
	     {
	         @Override
	         protected void initialize( Node created, Map<String, Object> properties )
	         {
	             created.setProperty( "search_term", properties.get( "search_term" ) );
	         }
	     };
	     
	     return factory.getOrCreate( "search_term", search_term );
	 }
   
    private static LinkedList <String> getpath( final Node Base , final int depth, Logger log4j)
    {
    	log4j.info("starting method : getpath");
 	   final StopEvaluator DEPTH_limited = new StopEvaluator()
 	    {
 	        public boolean isStopNode( final TraversalPosition currentPosition )
 	        {
 	            return currentPosition.depth() >= depth;
 	        }
 	    };
 	LinkedList <String> list = new LinkedList <String>();
 	
 	Traverser results = Base.traverse( Order.BREADTH_FIRST,
                DEPTH_limited,
                ReturnableEvaluator.ALL_BUT_START_NODE, RelTypes.PARENT,
                Direction.BOTH );
 		int count = 0;
    	for ( Node node : results )
    	{
    	    list.add(node.getProperty("search_term").toString());
    	    count++;
    	}
    	log4j.info(count + " relatives for search term: "  + Base.getProperty("search_term").toString() + " in the tree, with depth " + depth);
    	log4j.info("end method : getpath");
    	return list;
    	
    	
    }
    
    public static LinkedList <String> getallsearchterms( LinkedList <String> search_terms , int depth, Logger log4j)
    {
    	log4j.info("starting method : getallsearchterms");
    	Iterator<String> iter = search_terms.iterator();
    	LinkedList <String> result = new LinkedList <String>();
    	result.addAll(search_terms);
    	while (iter.hasNext())
    	{
    		//int countfornode = 0;
    		Node node = getOrCreateNodeWithUniqueFactory( iter.next(), graphDb );
    		LinkedList <String> templist = getpath(node , depth , log4j);
    		result.addAll(templist);
    	}
    	log4j.info("end method : getallsearchterms");
    	return result;
    }
     
   
   
   public static void addNode(String parent,String[] childs,Logger log4j )
   {
	    createDb(); 
	    Transaction tx = graphDb.beginTx();
   	 
        try
        {
        
         //log4j.info("start adding nodes, parent query is: " + parent + ", son is: " + child);
       	 Node par = getOrCreateNodeWithUniqueFactory( parent, graphDb );
       	 //Node son = getOrCreateNodeWithUniqueFactory( child, graphDb );
       	 boolean relationship_exists = false;
       	 LinkedList<String> sonstoadd = new LinkedList(Arrays.asList(childs));
       	 sonstoadd = new LinkedList<String>(new HashSet(sonstoadd));
       	 sonstoadd.remove(parent);
       	 //IndexManager index = graphDb.index();
       	 //RelationshipIndex pars = index.forRelationships( "org.neo4j.graphdb.Relationship" );
       	 //IndexHits<Relationship> relIndex;
       	 Iterable<Relationship> b = par.getRelationships(RelTypes.PARENT, Direction.OUTGOING);
       	 Iterator<Relationship> c = b.iterator();
       	 if (c.hasNext()){
       		 while (c.hasNext()){
       			 Node temp = c.next().getOtherNode(par);
       			 for (int i=0;i<childs.length;i++)
       			 {
       				if (String.valueOf(temp.getProperty("search_term")) == String.valueOf(childs[i]))
       				{
       					sonstoadd.remove(childs[i]);
       				}
       				
       			 }
           		 
           	 }
       	 }
   		 Iterator<String> sons = sonstoadd.iterator();
   		 while (sons.hasNext())
   		 {
   			 String son1 = sons.next();
   			 log4j.info("inserting new relationship, parent is:  " + parent + "  and son is:  " + son1);
   			 Node son = getOrCreateNodeWithUniqueFactory( son1, graphDb );
   			 Relationship pnt = par.createRelationshipTo(son, RelTypes.PARENT);
   			 pnt.setProperty("parent", 1);
   		 }
       		 
       		 
       	 
       	 
       	 
       	 
       	 
       	 
            

            tx.success();
        }
        finally
        {
        	//log4j.info("end add node");
            tx.finish();
            shutDown();
        }
   	
   }
}
