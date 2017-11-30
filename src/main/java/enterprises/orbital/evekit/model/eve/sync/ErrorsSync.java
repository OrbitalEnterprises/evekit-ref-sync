package enterprises.orbital.evekit.model.eve.sync;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import enterprises.orbital.evekit.model.AbstractRefSync;
import enterprises.orbital.evekit.model.AttributeSelector;
import enterprises.orbital.evekit.model.RefCachedData;
import enterprises.orbital.evekit.model.RefData;
import enterprises.orbital.evekit.model.RefSyncTracker;
import enterprises.orbital.evekit.model.RefSynchronizerUtil;
import enterprises.orbital.evekit.model.RefSynchronizerUtil.SyncStatus;
import enterprises.orbital.evekit.model.SyncTracker;
import enterprises.orbital.evekit.model.eve.ErrorType;
import enterprises.orbital.evexmlapi.IResponse;
import enterprises.orbital.evexmlapi.eve.IError;
import enterprises.orbital.evexmlapi.eve.IEveAPI;

public class ErrorsSync extends AbstractRefSync {
  protected static final Logger log = Logger.getLogger(ErrorsSync.class.getName());

  @Override
  public boolean isRefreshed(
                             RefSyncTracker tracker) {
    return tracker.getErrorListStatus() != SyncTracker.SyncState.NOT_PROCESSED;
  }

  @Override
  public long getExpiryTime(
                            RefData container) {
    return container.getErrorListExpiry();
  }

  @Override
  public void updateStatus(
                           RefSyncTracker tracker,
                           SyncTracker.SyncState status,
                           String detail) {
    tracker.setErrorListStatus(status);
    tracker.setErrorListDetail(detail);
    RefSyncTracker.updateTracker(tracker);
  }

  @Override
  public void updateExpiry(
                           RefData container,
                           long expiry) {
    container.setErrorListExpiry(expiry);
    RefCachedData.updateData(container);
  }

  @Override
  public boolean commit(
                        long time,
                        RefSyncTracker tracker,
                        RefData container,
                        RefCachedData item) {

    if (item instanceof ErrorType) {
      ErrorType api = (ErrorType) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        ErrorType existing = ErrorType.get(time, api.getErrorCode());

        if (existing != null) {
          if (!existing.equivalent(api)) {
            // Evolve
            existing.evolve(api, time);
            super.commit(time, tracker, container, existing);
            super.commit(time, tracker, container, api);
          }
        } else {
          // New entity
          api.setup(time);
          super.commit(time, tracker, container, api);
        }
      }
    } else {
      // Should never happen!
      assert false;
    }

    return true;
  }

  @Override
  protected Object getServerData(
                                 IResponse serverRequest)
    throws IOException {
    return ((IEveAPI) serverRequest).requestErrors();
  }

  @Override
  protected long processServerData(
                                   long time,
                                   IResponse serverRequest,
                                   Object data,
                                   List<RefCachedData> updates)
    throws IOException {
    @SuppressWarnings("unchecked")
    Collection<IError> errorList = (Collection<IError>) data;
    // Handle error list
    Set<Integer> seenErrors = new HashSet<>();
    for (IError nextError : errorList) {
      updates.add(new ErrorType(nextError.getErrorCode(), nextError.getErrorText()));
      seenErrors.add(nextError.getErrorCode());
    }
    // Look for stations which no longer exist and mark for deletion
    AttributeSelector ats = makeAtSelector(time);
    List<ErrorType> nextBatch = ErrorType.accessQuery(-1, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
    while (!nextBatch.isEmpty()) {
      for (ErrorType n : nextBatch) {
        if (!seenErrors.contains(n.getErrorCode())) {
          n.evolve(null, time);
          updates.add(n);
        }
      }
      nextBatch = ErrorType.accessQuery(nextBatch.get(nextBatch.size() - 1).getCid(), 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
    }
    return serverRequest.getCachedUntil().getTime();
  }

  private static final ErrorsSync syncher = new ErrorsSync();

  public static SyncStatus sync(
                                long time,
                                RefSynchronizerUtil syncUtil,
                                IResponse serverRequest) {
    return syncher.syncData(time, syncUtil, serverRequest, "ErrorList");
  }

  public static SyncStatus exclude(
                                   RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "ErrorList", SyncTracker.SyncState.SYNC_ERROR);
  }

  public static SyncStatus notAllowed(
                                      RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "ErrorList", SyncTracker.SyncState.NOT_ALLOWED);
  }

}
