package enterprises.orbital.evekit.model;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.base.PersistentProperty;
import enterprises.orbital.eve.esi.client.invoker.ApiException;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.evekit.account.EveKitRefDataProvider;
import org.apache.http.HttpStatus;
import org.apache.http.client.utils.DateUtils;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Abstract base class for ESI reference endpoint synchronizers.
 * <p>
 * In general, synchronization occurs as follows:
 * <p>
 * <ol>
 * <li>Get the current tracker. If no tracker exists, then exit.</li>
 * <li>Check whether the tracker has already been refreshed for the current data. If yes, then we're done and exit.</li>
 * <li>Check whether pre-reqs have been satisfied for the current data. If no, then queue up to try again later.</li>
 * <li>At this point, we proceed if the tracker is not done, the data is not expired and we're not waiting on any pre-reqs.</li>
 * <li>Interact with the EVE server to update data. If successful, create a data update object.</li>
 * <li>Retrieve the tracker again. If no tracker exists, then someone else refreshed this data and we're done.</li>
 * <li>Check whether the tracker has already been refreshed for the current data. If yes, then someone else refreshed this data and we're done.</li>
 * <li>Update the status and expiry of the tracker.</li>
 * <li>Merge any updates to the data, delete any data to be removed.</li>
 * <li>Create a new unfinished tracker for the next update based on cache expiry time.</li>
 * </ol>
 */
public abstract class AbstractESIRefSync<ServerDataType> implements ESIRefSynchronizationHandler {
  private static final Logger log = Logger.getLogger(AbstractESIRefSync.class.getName());

  // Default delay for future sync events
  private static final String PROP_DEFAULT_SYNC_DELAY = "enterprises.orbital.evekit.ref_sync_mgr.default_sync_delay";
  private static final long DEF_DEFAULT_SYNC_DELAY = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  // Default maximum delay for an in-progress synchronization
  private static final String PROP_MAX_DELAY = "enterprises.orbital.evekit.ref_sync_mgr.max_sync_delay";
  private static final long DEF_MAX_DELAY = TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES);

  // Batch size for bulk commits
  private static final String PROP_REF_COMMIT_BATCH_SIZE = "enterprises.orbital.evekit.ref_sync_mgr.commit_batch_size";
  private static final int DEF_REF_COMMIT_BATCH_SIZE = 200;

  // Convenient attribute selector which matches any attribute
  public static final AttributeSelector ANY_SELECTOR = new AttributeSelector("{ any: true }");

  // List of endpoints we should skip during synchronization (separate with '|')
  public static final String    PROP_EXCLUDE_SYNC               = "enterprises.orbital.evekit.ref.exclude_sync";

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

  // Convenience function to construct a time selector for the give time.
  public static AttributeSelector makeAtSelector(long time) {
    return new AttributeSelector("{values: [" + time + "]}");
  }

  // Interface which forwards a call to the class specific query function to retrieve data
  public interface QueryCaller<A extends RefCachedData> {
    List<A> query(long contid, AttributeSelector at) throws IOException;
  }

  /**
   * Retrieval all data items of the specified type live at the specified time.
   * This function continues to accumulate results until a query returns no results.
   *
   * @param time the "live" time for the retrieval.
   * @param query an interface which performs the type appropriate query call.
   * @param <A> class of the object which will be returned.
   * @return the list of results.
   * @throws IOException on any DB error.
   */
  @SuppressWarnings("Duplicates")
  public static <A extends RefCachedData> List<A> retrieveAll(long time, QueryCaller<A> query) throws IOException {
    final AttributeSelector ats = makeAtSelector(time);
    long contid = 0;
    List<A> results = new ArrayList<>();
    List<A> nextBatch = query.query(contid, ats);
    while (!nextBatch.isEmpty()) {
      results.addAll(nextBatch);
      contid = nextBatch.get(nextBatch.size() - 1).getCid();
      nextBatch = query.query(contid, ats);
    }
    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getContext() {
    return "[" + getClass().getSimpleName() + "]";
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ESIRefEndpointSyncTracker getCurrentTracker() throws IOException, TrackerNotFoundException {
    return ESIRefEndpointSyncTracker.getUnfinishedTracker(endpoint());
  }

  /**
   * A default time in the future when the next event for this handler should be scheduled.  This method
   * is called when it is otherwise not possible to determine an appropriate next event time.  This normally
   * happens when an error occurs during synchronization.  Sub-classes should override as appropriate.
   *
   * @return a time in the future when the next synchronization should be scheduled.
   */
  protected long defaultNextEvent() {
    return OrbitalProperties.getCurrentTime() + OrbitalProperties.getLongGlobalProperty(PROP_DEFAULT_SYNC_DELAY, DEF_DEFAULT_SYNC_DELAY);
  }

  /**
   * {@inheritDoc}
   */
  public long maxDelay() {
    return PersistentProperty.getLongPropertyWithFallback(PROP_MAX_DELAY, DEF_MAX_DELAY);
  }

  /**
   * Check whether any pre-requisites have been satisfied.  Sub-classes should override as appropriate.
   *
   * @return true if all pre-reqs have been satisfied, false otherwise.
   */
  protected boolean prereqSatisfied() {
    return true;
  }

  /**
   * Commit a data item at the specified synchronization time.  Sub-classes will normally override this method
   * and check whether it is necessary to update or evolve an existing item.
   *
   * @param time synchronization time at which this update will occur.
   * @param item item to update or commit
   * @throws IOException on any error (usually a database error)
   */
  protected void commit(
      long time,
      RefCachedData item) throws IOException {
    RefCachedData.update(item);
  }

  /**
   * Retrieve server data needed to process this update.  We structure the retrieval of server data in this way
   * to allow for uniform handling of client errors.
   *
   * @return a mostly opaque object containing server data to be used for the update.
   * @throws ApiException if a client error occurs while retrieving data.
   * @throws IOException  on any other error which occurs while retrieving data.
   */
  protected abstract ESIRefServerResult<ServerDataType> getServerData(ESIRefClientProvider cp)
      throws ApiException, IOException;

  /**
   * Process server data.  Normally, the subclass will extract server data into appropriate types
   * which are added to the update list (and later processed in the "commit" call).
   *
   * @param time    synchronization time.
   * @param data    server result previously retrieved via getServerData
   * @param updates list of objects to be updated as a result of processing.
   * @throws IOException on any error which occurs while processing server data
   */
  protected abstract void processServerData(
      long time,
      ESIRefServerResult<ServerDataType> data,
      List<RefCachedData> updates)
      throws IOException;

  /**
   * Convenience method for handling the common case where we should commit and EOL item
   * (if update.getLifeStart() != 0), evolve an existing item if it is different from an
   * update, or initialize and store a new item if no existing item is present.
   *
   * @param time     synchronization time at which this update will occur.
   * @param existing existing data item, if any.
   * @param update   new data item.
   * @throws IOException on any database error
   */
  protected void evolveOrAdd(long time, RefCachedData existing, RefCachedData update) throws IOException {
    if (update.getLifeStart() != 0) {
      // Existing element that is end of life (basically a delete).
      RefCachedData.update(update);
    } else if (existing != null) {
      if (!existing.equivalent(update)) {
        // Evolve
        existing.evolve(update, time);
        RefCachedData.update(existing);
        RefCachedData.update(update);
      }
    } else {
      // New entity
      update.setup(time);
      RefCachedData.update(update);
    }
  }

  /**
   * Utility method to extract expiry time from an ESI ApiResponse into milliseconds since the epoch UTC.
   *
   * @param result the ApiResponse which may contain an "expires" header.
   * @param def    value to return if header does not contain "expires" or the header can not be parsed properly.
   * @return expires header in milliseconds UTC, or the default.
   */
  protected static long extractExpiry(ApiResponse<?> result, long def) {
    try {
      String expireHeader = result.getHeaders()
                                  .get("Expires")
                                  .get(0);
      return DateUtils.parseDate(expireHeader)
                      .getTime();
    } catch (Exception e) {
      log.log(Level.FINE, "Error parsing header, will return default: " + def, e);
    }
    return def;
  }

  /**
   * Utility method to check for common problems with API responses.  The current list of common problems are:
   *
   * <ul>
   *   <li>A return code other than 200.</li>
   *   <li>A null data response.</li>
   * </ul>
   *
   * @param response the API response to check.
   * @throws IOException if a common problem is found in the response.
   */
  protected static void checkCommonProblems(ApiResponse<?> response) throws IOException {
    if (response.getStatusCode() != HttpStatus.SC_OK) throw new IOException("Unexpected return code: " + response.getStatusCode());
    if (response.getData() == null) throw new IOException("Response data is null");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void synch(ESIRefClientProvider cp) {
    log.fine("Starting synchronization: " + getContext());

    try {
      // Get the current tracker.  If no tracker exists, then we'll exit in the catch block below.
      ESIRefEndpointSyncTracker tracker = getCurrentTracker();

      // If the tracker is already refreshed, then exit
      if (tracker.isRefreshed()) {
        log.fine("Tracker is already refreshed: " + getContext());
        return;
      }

      // Check whether this tracker has been in progress too long.  If so, then close it down and exit.
      if (tracker.getSyncStart() > 0) {
        long delaySinceStart = OrbitalProperties.getCurrentTime() - tracker.getSyncStart();
        if (delaySinceStart > maxDelay()) {
          log.fine("Forcing tracker " + tracker + " to terminate due to delay: " + getContext());
          tracker.setStatus(ESISyncState.WARNING);
          tracker.setDetail("Terminated due to excessive delay");
          ESIRefEndpointSyncTracker.finishTracker(tracker);
          return;
        }
      }

      // Verify all pre-reqs have been satisfied.  If not then exit and scheduler will try again later.
      if (!prereqSatisfied()) {
        log.fine("Pre-reqs not satisfied: " + getContext());
        return;
      }

      // Start sync for this endpoint
      if (tracker.getSyncStart() <= 0) {
        tracker.setSyncStart(OrbitalProperties.getCurrentTime());
        tracker = EveKitRefDataProvider.update(tracker);
      }

      // Set syncTime to the start of the current tracker
      long syncTime = tracker.getSyncStart();
      long nextEvent;

      try {
        // Retrieve server and process server data.  Any client or processing
        // errors will result in marking the tracker as in error with an endpoint
        // specific time for the next scheduled event.  Otherwise, the schedule time
        // returned by the data processor is used.
        List<RefCachedData> updateList = new ArrayList<>();
        log.fine("Retrieving server data: " + getContext());
        ESIRefServerResult<ServerDataType> serverData = getServerData(cp);
        nextEvent = serverData.getExpiryTime();
        log.fine("Processing server data: " + getContext());
        processServerData(syncTime, serverData, updateList);

        // Commit all updates.  We process updates in batches with sizes that can be varied dynamically by the
        // admin as needed.  Smaller batches prevent long running transactions from tying up contended resources.
        log.fine("Storing updates: " + getContext());
        int batchSize = PersistentProperty.getIntegerPropertyWithFallback(PROP_REF_COMMIT_BATCH_SIZE, DEF_REF_COMMIT_BATCH_SIZE);
        int count = updateList.size();
        if (count > 0) {
          log.fine("Processing " + updateList.size() + " total updates: " + getContext());
          for (int i = 0, endIndex = Math.min(i + batchSize, count); i < count; i = endIndex, endIndex = Math.min(i + batchSize, count)) {
            List<RefCachedData> nextBlock = updateList.subList(i, endIndex);
            try {
              EveKitRefDataProvider.getFactory().runTransaction(() -> {
                // Handle next block of commits.
                log.fine("Processing " + nextBlock.size() + " updates: " + getContext());
                long start = OrbitalProperties.getCurrentTime();
                for (RefCachedData obj : nextBlock) {
                    commit(syncTime, obj);
                }
                long end = OrbitalProperties.getCurrentTime();
                if (log.isLoggable(Level.FINE)) {
                  // Commit commit rate if FINE if debugging
                  long delay = end - start;
                  double rate = delay / (double) nextBlock.size();
                  log.fine("Process rate = " + rate + " milliseconds/update: " + getContext());
                }
              });
            } catch (Exception e) {
              if (e.getCause() instanceof IOException) throw (IOException) e.getCause();
              log.log(Level.SEVERE, "query error: " + getContext(), e);
              throw new IOException(e.getCause());
            }
          }
        }

        log.fine("Update and store finished normally: " + getContext());
        tracker.setStatus(ESISyncState.FINISHED);
        tracker.setDetail("Updated successfully");
      } catch (ApiException e) {
        // Client error while updating, mark the error in the tracker and exit
        log.log(Level.WARNING, "ESI client error: " + getContext(), e);
        nextEvent = -1;
        tracker.setStatus(ESISyncState.ERROR);
        tracker.setDetail("ESI client error, contact the site admin if this problem persists");
      } catch (IOException e) {
        // Other error while updating, mark the error in the tracker and exit
        // Database errors during the update should end up here.
        log.log(Level.WARNING, "Error during update: " + getContext(), e);
        nextEvent = -1;
        tracker.setStatus(ESISyncState.ERROR);
        tracker.setDetail("Server error, contact the site admin if this problem persists");
      }

      // Complete the tracker
      ESIRefEndpointSyncTracker.finishTracker(tracker);

      // Schedule the next event
      nextEvent = nextEvent < 0 ? defaultNextEvent() : nextEvent;
      ESIRefEndpointSyncTracker.getOrCreateUnfinishedTracker(endpoint(), nextEvent);

    } catch (TrackerNotFoundException e) {
      // No action to take, exit
      log.fine("No unfinished tracker: " + getContext());
    } catch (IOException e) {
      // Database errors during the update or access to the tracker will end up here.
      log.log(Level.WARNING, "Error during synchronization, tracker may not be updated: " + getContext(), e);
    }
  }

  // Convenience methods for dealing with missing data from api calls
  public static int nullSafeInteger(Integer value, int def) {
    if (value == null) return def;
    return value.intValue();
  }

  public static long nullSafeLong(Long value, long def) {
    if (value == null) return def;
    return value.longValue();
  }

  public static float nullSafeFloat(Float value, float def) {
    if (value == null) return def;
    return value.floatValue();
  }

  public static DateTime nullSafeDateTime(DateTime value, DateTime def) {
    if (value == null) return def;
    return value;
  }
}
