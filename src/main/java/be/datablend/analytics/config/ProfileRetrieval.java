package be.datablend.analytics.config;

import com.google.gdata.client.analytics.AnalyticsService;
import com.google.gdata.data.analytics.AccountEntry;
import com.google.gdata.data.analytics.AccountFeed;
import com.google.gdata.util.AuthenticationException;
import com.google.gdata.util.ServiceException;

import java.io.IOException;
import java.net.URL;

/**
 * User: dsuvee
 * Date: 08/03/12
 */
public class ProfileRetrieval {

    private AnalyticsService analyticsService;

    public ProfileRetrieval(String username, String password) throws AuthenticationException {
        analyticsService = new AnalyticsService(Configuration.SERVICE);
        analyticsService.setUserCredentials(username, password);
    }

    // Retrieve the available profiles for the specific user
    public void obtainProfiles() throws IOException, ServiceException {
        AccountFeed accountFeed = analyticsService.getFeed(new URL(Configuration.ACCOUNT_URL), AccountFeed.class);
        for (AccountEntry accountEntry : accountFeed.getEntries()) {
            System.out.println(accountEntry.getTitle().getPlainText() + " "  + accountEntry.getTableId().getValue());
        }
    }

    public static void main(String[] args) throws ServiceException, IOException {
        ProfileRetrieval wrap = new ProfileRetrieval(Configuration.CLIENT_USERNAME, Configuration.CLIENT_PASS);
        wrap.obtainProfiles();
    }

}
