package be.datablend.analytics.query;

import be.datablend.analytics.model.Navigation;
import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.cypher.ExecutionResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import java.net.UnknownHostException;
import java.util.*;

/**
 * User: dsuvee
 * Date: 08/03/12
 */
public class Query {

    private EmbeddedGraphDatabase graphDb;
    private ExecutionEngine engine;

    public Query() throws UnknownHostException {
        // Init the graph connection
        graphDb = new EmbeddedGraphDatabase("var/analytics");
        engine = new ExecutionEngine(graphDb);
        // Print some statistics
        int numberofpaths = 0;
        int numberofnavigations = 0;
        Iterator<Node> nodeIterator = graphDb.getAllNodes().iterator();
        while (nodeIterator.hasNext()) {
            Node node = nodeIterator.next();
            numberofpaths = numberofpaths + 1;
            // Retrieve the relationships for that path
            Iterator<Relationship> relationshipIterator = node.getRelationships(Direction.OUTGOING).iterator();
            while (relationshipIterator.hasNext()) {
                relationshipIterator.next();
                numberofnavigations = numberofnavigations + 1;
            }
        }
        System.out.println("Number of paths: " + numberofpaths);
        System.out.println("Number of navigations: " + numberofnavigations + "\n");

    }

    // Create the circos data set
    public void getCircosData(Date from, Date to, int threshold) throws UnknownHostException {
        // Create the parameters
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("fromdate", from.getTime());
        params.put("todate", to.getTime());
        // Execute the query
        ExecutionResult result = engine.execute("START sourcepath=node:index(\"path:*\") " +
                                                 "MATCH sourcepath-[r]->targetpath " +
                                                 "WHERE r.date >= {fromdate} AND r.date <= {todate} " +
                                                 "RETURN sourcepath,targetpath",
                                                 params);

        // Retrieve the results
        Iterator<Map<String, Object>> it = result.javaIterator();
        List<Navigation> navigations = new ArrayList<Navigation>();
        Map<String,String> titles = new HashMap<String,String>();
        Set<String> paths = new HashSet<String>();
        // Iterate the results
        while (it.hasNext()) {
            Map<String, Object> record = it.next();
            String source = (String)((Node) record.get("sourcepath")).getProperty("path");
            String target = (String) ((Node) record.get("targetpath")).getProperty("path");
            String targettitle = (String) ((Node) record.get("targetpath")).getProperty("title");
            // Reuse the navigation object as temorary holder
            navigations.add(new Navigation(source, target, targettitle, new Date(), 1));
            paths.add(source);
            paths.add(target);
            if (!titles.containsKey(target)) {
                titles.put(target, targettitle);
            }
        }

        // Retrieve the various paths
        List<String> pathids = Arrays.asList(paths.toArray(new String[]{}));
        // Create the matrix that holds the info
        int[][] occurences = new int[pathids.size()][pathids.size()];

        // Iterate through all the navigations and update accordingly
        for (Navigation navigation : navigations) {
            int sourceindex = pathids.indexOf(navigation.getSource());
            int targetindex = pathids.indexOf(navigation.getTarget());
            occurences[sourceindex][targetindex] = occurences[sourceindex][targetindex] + 1;
        }

        // Matrix build, filter on threshold
        for (int i = 0; i < occurences.length; i++) {
            for (int j = 0; j < occurences.length; j++) {
                if (occurences[i][j] < threshold) {
                    occurences[i][j] = 0;
                }
            }
        }

        // Print the data
        printCircosData(pathids, titles, occurences);
    }

    // Helper method to print the circos data
    private void printCircosData(List<String> pathids, Map<String,String> titles, int[][] occurences) {
        // First print the legend
        int i = 0;
        for (String pathid : pathids) {
            System.out.println("link" + i + " - " + pathid + " - " + titles.get(pathid));
            i = i + 1;
        }
        System.out.println();

        // Print the header
        i = 0;
        System.out.print("data");
        for (String pathid : pathids) {
            System.out.print("\t" + "l" + i);
            i = i + 1;
        }
        System.out.println("");

        // Print the occurences themselves
        int j = 0;
        for (String sourcepathid : pathids) {
            System.out.print("l" + j);
            j = j + 1;
            for (String targetpath : pathids) {
                System.out.print("\t" + occurences[pathids.indexOf(sourcepathid)][pathids.indexOf(targetpath)]);
            }
            System.out.println();
        }
    }


    public static void main(String[] args) throws UnknownHostException {
        Query query = new Query();

                Calendar start = Calendar.getInstance();
        start.clear();
        start.set(Calendar.YEAR, 2011);
        start.set(Calendar.MONTH, 5);
        start.set(Calendar.DATE, 1);

        Calendar stop = Calendar.getInstance();
        stop.clear();
        stop.set(Calendar.YEAR, 2012);
        stop.set(Calendar.MONTH, 2);
        stop.set(Calendar.DATE, 31);

        query.getCircosData(start.getTime(), stop.getTime(), 10);
    }

}
