package enterprises.orbital.evekit.model;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.base.PersistentProperty;
import enterprises.orbital.db.ConnectionFactory.RunInTransaction;
import enterprises.orbital.evekit.account.EveKitRefDataProvider;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class for implementing the reference data model synchronization pattern using ESI.
 * In general, synchronization occurs as follows:
 *
 * <ol>
 *   <li>Get the current tracker. If no tracker exists, then exit.</li>
 *   <li>Check whether the tracker has already been refreshed for the current data. If yes, then we're done and exit.</li>
 *   <li>Check whether pre-reqs have been satisfied for the current data. If no, then queue up to try again later.</li>
 *   <li>Attempt to get a container for the current data. If missing, exit with "missing".</li>
 *   <li>Check whether the current data is expired. If no, then exit with "not expired".</li>
 *   <li>At this point, we proceed if the tracker is not done, the data is not expired, the container is not missing and we're not waiting on any pre-reqs.</li>
 *   <li>Interact with the EVE server to update data. If successful, create a data update object.</li>
 *   <li>Retrieve the tracker again. If no tracker exists, then someone else refreshed this data and we're done.</li>
 *   <li>Check whether the tracker has already been refreshed for the current data. If yes, then someone else refreshed this data and we're done.</li>
 *   <li>Attempt to get a container for the current data. If missing, exit with severe error.</li>
 *   <li>Update the status and expiry of the tracker.</li>
 *   <li>Merge any updates to the data, delete any data to be removed.</li>
 * </ol>
 */
public class ESIRefSynchronizerUtil {
  protected static final Logger log               = Logger.getLogger(ESIRefSynchronizerUtil.class.getName());

  // List of endpoints we should skip during synchronization (separate with '|')
  public static final String    PROP_EXCLUDE_SYNC               = "enterprises.orbital.evekit.ref.exclude_sync";

  // Minimum number of milliseconds that must elapse between attempts to synch reference data
  public static final String    PROP_SYNC_ATTEMPT_SEPARATION = "enterprises.orbital.evekit.ref.sync_attempt_separation";
  // Maximum number of milliseconds a tracker is allowed to remain unfinished
  public static final String    PROP_SYNC_TERM_DELAY         = "enterprises.orbital.evekit.ref.sync_terminate_delay";
  // XML API server connection timeout max (milliseconds)
  public static final String    PROP_CONNECT_TIMEOUT         = "enterprises.orbital.evekit.timeout.connect";
  // XML API server connection read timeout max (milliseconds)
  public static final String    PROP_READ_TIMEOUT            = "enterprises.orbital.evekit.timeout.read";
  // Agent to set for all XML API requests
  public static final String    PROP_SITE_AGENT              = "enterprises.orbital.evekit.site_agent";
  // XML API URL to use
  public static final String    PROP_XML_API_URL             = "enterprises.orbital.evekit.api_server_url";

  public static final int       COMMIT_BATCH_SIZE = 200;

  /**
   * Retrieve the ESI endpoints that have been excluded from synchronization by the admin.
   *
   * @return the set of excluded ESI endpoints.
   */
  public static Set<ESIRefSyncEndpoint> getExcludedEndpoints() {
    String[] excludedStates = PersistentProperty.getPropertyWithFallback(PROP_EXCLUDE_SYNC, "")
                                                .split("\\|");
    Set<ESIRefSyncEndpoint> excluded = new HashSet<>();
    for (String next : excludedStates) {
      if (!next.isEmpty()) {
        try {
          ESIRefSyncEndpoint val = ESIRefSyncEndpoint.valueOf(next);
          excluded.add(val);
        } catch (IllegalArgumentException e) {
          // Unknown value type, skip
          log.warning("Unknown endpoint name " + next + ", ignoring.");
        }
      }
    }
    return excluded;
  }

}
