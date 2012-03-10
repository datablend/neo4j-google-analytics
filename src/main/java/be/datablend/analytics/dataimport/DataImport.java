package be.datablend.analytics.dataimport;

import be.datablend.analytics.model.Navigation;
import be.datablend.analytics.model.Relationships;
import com.google.gdata.util.ServiceException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

/**
 * User: dsuvee
 * Date: 08/03/12
 */
public class DataImport {

    private EmbeddedGraphDatabase graphDb;
    private IndexManager indexManager = null;
	private Index<Node> index = null;

    public DataImport() {
        // Init the graph connection
        graphDb = new EmbeddedGraphDatabase("var/analytics");
        indexManager = graphDb.index();
		// Create one index
		index = indexManager.forNodes("index");
    }

    // Imports all analytics data between two dates in a graph database
    public void importData(Date startDate, Date stopDate) throws IOException, ParseException, ServiceException {
        // Create the stub
        AnalyticsDataRetrieval retrieval = new AnalyticsDataRetrieval();
        // Retrieve the list of dates
        List<Date> dates = getDates(startDate, stopDate);
        // Add data for all dates
        for (Date date : dates) {
            // Get the individual navigations
            System.out.println("Importing data for : " + date);
            List<Navigation> navigations = retrieval.getNavigations(date);
            // Save them in the graph database
            Transaction tx = graphDb.beginTx();
            // Add all of the tick one by one
            for (Navigation nav : navigations) {
                Node source = getPath(nav.getSource());
                Node target = getPath(nav.getTarget());
                if (!target.hasProperty("title")) {
                    target.setProperty("title", nav.getTargetTitle());
                }
                for (long i = 0; i < nav.getAmount(); i++) {
                    // Duplicate relationships
                    Relationship transition = source.createRelationshipTo(target, Relationships.NAVIGATION);
                    transition.setProperty("date", date.getTime()); // Save time as long
                }
            }
            tx.success();
            tx.finish();
        }
    }

    // Helper method to find a specific path
    private Node getPath(String path) {
        String query = "path:\"" + path + "\"";
		IndexHits<Node> results = index.query(query);
        Node found = results.getSingle();
        if (found == null) {
            found = graphDb.createNode();
            found.setProperty("path", path);
            index.add(found, "path", path);
        }
        return found;
    }

    // Helper method to retrieves the list of relevant dates
    private List<Date> getDates(Date startDate, Date stopDate) {
        List<Date> dates = new ArrayList<Date>();
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(startDate);
        while (calendar.getTime().before(stopDate)) {
            dates.add(calendar.getTime());
            calendar.add(Calendar.DATE, 1);
        }
        return dates;
    }

    // Import the data itself
    public static void main(String [] args) throws IOException, ParseException, ServiceException {
        DataImport dataimport = new DataImport();

        Calendar start = Calendar.getInstance();
        start.clear();
        start.set(Calendar.YEAR, 2011);
        start.set(Calendar.MONTH, 5);
        start.set(Calendar.DATE, 1);

        Calendar stop = Calendar.getInstance();
        stop.clear();
        stop.set(Calendar.YEAR, 2012);
        stop.set(Calendar.MONTH, 3);
        stop.set(Calendar.DATE, 31);

        dataimport.importData(start.getTime(), stop.getTime());
    }


}
