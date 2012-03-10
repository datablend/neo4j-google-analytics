package be.datablend.analytics.dataimport;

import be.datablend.analytics.config.Configuration;
import be.datablend.analytics.model.Navigation;
import com.google.gdata.client.analytics.AnalyticsService;
import com.google.gdata.client.analytics.DataQuery;
import com.google.gdata.data.analytics.DataEntry;
import com.google.gdata.data.analytics.DataFeed;
import com.google.gdata.util.AuthenticationException;
import com.google.gdata.util.ServiceException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * User: dsuvee
 * Date: 08/03/12
 */
public class AnalyticsDataRetrieval {

    private static final String BASE_URL = "https://www.google.com/analytics/feeds/data";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    private AnalyticsService analyticsService;

    public AnalyticsDataRetrieval() throws AuthenticationException, IOException, ServiceException {
        analyticsService = new AnalyticsService(Configuration.SERVICE);
        analyticsService.setUserCredentials(Configuration.CLIENT_USERNAME, Configuration.CLIENT_PASS);
    }

    // Retrieves all navigations for a particular date
    public List<Navigation> getNavigations(Date date) throws IOException, ServiceException {
        // Execute the query
        DataFeed feed = analyticsService.getFeed(createQueryUrl(date), DataFeed.class);

        // Retrieve the information
        List<Navigation> navigations = new ArrayList<Navigation>();

        // Transform and clean
        for (DataEntry entry : feed.getEntries()) {
            String pagepath = entry.stringValueOf("ga:pagePath");
            String pagetitle = entry.stringValueOf("ga:pageTitle");
            String previouspagepath = entry.stringValueOf("ga:previousPagePath");
            String medium = entry.stringValueOf("ga:medium");
            long views = entry.longValueOf("ga:pageviews");
            // Filter the data
            if (filter(pagepath) && filter(previouspagepath) && (!clean(previouspagepath).equals(clean(pagepath)))) {
                // If criteria are satisfied, save it
                Navigation navigation =  new Navigation(clean(previouspagepath), clean(pagepath), pagetitle, date, views);
                if (navigation.getSource().equals("(entrance)")) {
                    // In case of an entrace, save its medium instead
                    navigation.setSource(medium);
                }
                navigations.add(navigation);
            }
        }
        return navigations;
    }

    // Filter paths
    private boolean filter(String path) {
        //return true;
        return path.startsWith("/?p") || path.startsWith("(entrance)");
    }

    // Clean paths
    public String clean(String path) {
        if (path.startsWith("/?p")) {
            if (path.contains("&")) {
                return path.substring(0,path.indexOf("&"));
            }
        }
        return path;
    }

    // Creates the specific url to retrieve the relevant information
    private URL createQueryUrl(Date date) throws MalformedURLException {
        // Transform to string
        String datestring = DATE_FORMAT.format(date);
        // Create the query itself
        DataQuery query = new DataQuery(new URL(Configuration.DATA_URL));
        query.setIds(Configuration.TABLE_ID);
        query.setDimensions("ga:medium,ga:previousPagePath,ga:pagePath,ga:pageTitle");
        query.setMetrics("ga:pageviews");
        query.setStartDate(datestring);
        query.setEndDate(datestring);
        return query.getUrl();
    }

}