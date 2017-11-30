package enterprises.orbital.evekit.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import enterprises.orbital.evekit.model.RefSynchronizerUtil.SyncStatus;
import enterprises.orbital.evexmlapi.IResponse;

public abstract class AbstractRefSync implements RefSynchronizationHandler {
  private static final Logger              log          = Logger.getLogger(AbstractRefSync.class.getName());

  protected static final AttributeSelector ANY_SELECTOR = new AttributeSelector("{ any: true }");

  protected static AttributeSelector makeAtSelector(
                                                    long time) {
    return new AttributeSelector("{values: [" + time + "]}");
  }

  public static boolean StringChanged(
                                      String a,
                                      String b) {
    return !String.valueOf(a).equals(String.valueOf(b));
  }

  @Override
  public RefSyncTracker getCurrentTracker() {
    return RefSyncTracker.getUnfinishedTracker();
  }

  @Override
  public RefData getExistingContainer() {
    return RefData.getRefData();
  }

  @Override
  public boolean prereqSatisfied(
                                 RefSyncTracker tracker) {
    return true;
  }

  @Override
  public boolean commit(
                        long time,
                        RefSyncTracker tracker,
                        RefData container,
                        final RefCachedData item) {
    return RefCachedData.updateData(item) != null;
  }

  protected abstract Object getServerData(
                                          IResponse serverRequest)
    throws IOException;

  /**
   * Handle a server error and return the appropriate SyncState we should store in this case. The default behavior is to log the error and return SYNC_ERROR.
   * 
   * @param serverRequest
   *          the interface we used to make the request. Can be interrogated for error detail.
   * @param errorDetail
   *          string builder which should be populated with error detail we report in the sync status.
   * @return the SyncState we should store as a result of this error.
   */
  protected static SyncTracker.SyncState handleServerError(
                                                           IResponse serverRequest,
                                                           StringBuilder errorDetail) {
    switch (serverRequest.getErrorCode()) {
    case 222:
    case 221:
    case 220:
    case 125:
      // Treat all these errors as warnings
      errorDetail.append("Warning ").append(serverRequest.getErrorCode()).append(": ").append(serverRequest.getErrorString());
      return SyncTracker.SyncState.SYNC_WARNING;

    default:
      // Everything else is treated as an error
      errorDetail.append("Error ").append(serverRequest.getErrorCode()).append(": ").append(serverRequest.getErrorString());
      return SyncTracker.SyncState.SYNC_ERROR;
    }
  }

  protected abstract long processServerData(
                                            long time,
                                            IResponse serverRequest,
                                            Object data,
                                            List<RefCachedData> updates)
    throws IOException;

  public SyncStatus syncData(
                             long time,
                             RefSynchronizerUtil syncUtil,
                             IResponse serverRequest,
                             String description) {

    try {
      // Run pre-check.
      SyncStatus preCheck = syncUtil.preSyncCheck(description, this);
      if (preCheck != SyncStatus.CONTINUE) return preCheck;
      log.fine("Starting refresh request for " + description);
      SyncTracker.SyncState status = SyncTracker.SyncState.UPDATED;
      String errorDetail = null;
      long nextExpiry = -1;
      List<RefCachedData> updateList = new ArrayList<RefCachedData>();

      try {
        Object serverData = getServerData(serverRequest);

        if (serverRequest.isError()) {
          StringBuilder errStr = new StringBuilder();
          status = handleServerError(serverRequest, errStr);
          errorDetail = errStr.toString();
          if (status == SyncTracker.SyncState.SYNC_ERROR) log.warning("request failed: " + errorDetail);
        } else {
          nextExpiry = processServerData(time, serverRequest, serverData, updateList);
        }

      } catch (IOException e) {
        status = SyncTracker.SyncState.SYNC_ERROR;
        errorDetail = "request failed with IO error";
        log.log(Level.WARNING, "request failed with error", e);
      }

      log.fine("Completed refresh request for " + description);

      // Finished sync, store result if needed
      syncUtil.storeSynchResults(time, status, errorDetail, nextExpiry, description, updateList, this);
      return SyncStatus.DONE;

    } catch (IOException e) {
      // Log but give us another shot in the queue
      log.warning("store failed with error " + e);
      return SyncStatus.ERROR;
    }

  }

  /**
   * Exclude synchronization of the current state. We exclude a state by recording it as failed with a message indicating that this state was excluded.
   * 
   * @param syncUtil
   *          the utility to use for synchronization actions.
   * @param description
   *          a description of the synchronization operation being performed.
   * @param state
   *          the synchronization state to record in the tracker
   * @return the outcome of the exclusion. Normally, this will be HttpServletResponse.SC_OK unless there is an error updating the state.
   */
  protected SyncStatus excludeState(
                                    RefSynchronizerUtil syncUtil,
                                    String description,
                                    SyncTracker.SyncState state) {

    try {
      // Verify we havnen't already updated this state.
      SyncStatus preCheck = syncUtil.preSyncCheck(description, this);
      if (preCheck != SyncStatus.CONTINUE) return preCheck;

      // Record the exclusion.
      log.fine("Exluding synchronization for " + description);
      SyncTracker.SyncState status = state;
      String errorDetail = "";
      switch (status) {
      case NOT_ALLOWED:
        errorDetail = "API key for this account does not allow synchronization of this data.  Contact the site admin if you think this is an error.";
        break;

      case SYNC_ERROR:
      default:
        errorDetail = "Synchronization skipped due to admin exclusion.  Contact the site admin for more info.";
        break;
      }

      // NOTE: time argument not relevant here since we're only updating tracker
      syncUtil.storeSynchResults(0L, status, errorDetail, -1, description, null, this);

      return SyncStatus.DONE;

    } catch (IOException e) {
      // Log but give us another shot in the queue
      log.warning("store failed with error " + e);
      return SyncStatus.ERROR;
    }

  }

}
