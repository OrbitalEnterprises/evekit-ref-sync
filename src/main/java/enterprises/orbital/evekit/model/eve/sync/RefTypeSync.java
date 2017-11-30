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
import enterprises.orbital.evekit.model.eve.RefType;
import enterprises.orbital.evexmlapi.IResponse;
import enterprises.orbital.evexmlapi.eve.IEveAPI;
import enterprises.orbital.evexmlapi.eve.IRefType;

public class RefTypeSync extends AbstractRefSync {
  protected static final Logger log = Logger.getLogger(RefTypeSync.class.getName());

  @Override
  public boolean isRefreshed(
                             RefSyncTracker tracker) {
    return tracker.getRefTypeStatus() != SyncTracker.SyncState.NOT_PROCESSED;
  }

  @Override
  public long getExpiryTime(
                            RefData container) {
    return container.getRefTypeExpiry();
  }

  @Override
  public void updateStatus(
                           RefSyncTracker tracker,
                           SyncTracker.SyncState status,
                           String detail) {
    tracker.setRefTypeStatus(status);
    tracker.setRefTypeDetail(detail);
    RefSyncTracker.updateTracker(tracker);
  }

  @Override
  public void updateExpiry(
                           RefData container,
                           long expiry) {
    container.setRefTypeExpiry(expiry);
    RefCachedData.updateData(container);
  }

  @Override
  public boolean commit(
                        long time,
                        RefSyncTracker tracker,
                        RefData container,
                        RefCachedData item) {

    if (item instanceof RefType) {
      RefType api = (RefType) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        RefType existing = RefType.get(time, api.getRefTypeID());

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
    return ((IEveAPI) serverRequest).requestRefTypes();
  }

  @Override
  protected long processServerData(
                                   long time,
                                   IResponse serverRequest,
                                   Object data,
                                   List<RefCachedData> updates)
    throws IOException {
    @SuppressWarnings("unchecked")
    Collection<IRefType> refTypeList = (Collection<IRefType>) data;
    // Handle error list
    Set<Integer> seenTypes = new HashSet<>();
    for (IRefType nextRefType : refTypeList) {
      updates.add(new RefType(nextRefType.getRefTypeID(), nextRefType.getRefTypeName()));
      seenTypes.add(nextRefType.getRefTypeID());
    }
    // Look for ref types which no longer exist and mark for deletion
    AttributeSelector ats = makeAtSelector(time);
    List<RefType> nextBatch = RefType.accessQuery(-1, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
    while (!nextBatch.isEmpty()) {
      for (RefType n : nextBatch) {
        if (!seenTypes.contains(n.getRefTypeID())) {
          n.evolve(null, time);
          updates.add(n);
        }
      }
      nextBatch = RefType.accessQuery(nextBatch.get(nextBatch.size() - 1).getCid(), 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
    }
    return serverRequest.getCachedUntil().getTime();
  }

  private static final RefTypeSync syncher = new RefTypeSync();

  public static SyncStatus sync(
                                long time,
                                RefSynchronizerUtil syncUtil,
                                IResponse serverRequest) {
    return syncher.syncData(time, syncUtil, serverRequest, "RefType");
  }

  public static SyncStatus exclude(
                                   RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "RefType", SyncTracker.SyncState.SYNC_ERROR);
  }

  public static SyncStatus notAllowed(
                                      RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "RefType", SyncTracker.SyncState.NOT_ALLOWED);
  }

}
