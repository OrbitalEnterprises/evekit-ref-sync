package enterprises.orbital.evekit.model.map.sync;

import java.io.IOException;
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
import enterprises.orbital.evekit.model.map.Sovereignty;
import enterprises.orbital.evexmlapi.IResponse;
import enterprises.orbital.evexmlapi.map.IMapAPI;
import enterprises.orbital.evexmlapi.map.ISovereignty;
import enterprises.orbital.evexmlapi.map.ISystemSovereignty;

public class SovereigntySync extends AbstractRefSync {
  protected static final Logger log = Logger.getLogger(SovereigntySync.class.getName());

  @Override
  public boolean isRefreshed(
                             RefSyncTracker tracker) {
    return tracker.getSovereigntyStatus() != SyncTracker.SyncState.NOT_PROCESSED;
  }

  @Override
  public long getExpiryTime(
                            RefData container) {
    return container.getSovereigntyExpiry();
  }

  @Override
  public void updateStatus(
                           RefSyncTracker tracker,
                           SyncTracker.SyncState status,
                           String detail) {
    tracker.setSovereigntyStatus(status);
    tracker.setSovereigntyDetail(detail);
    RefSyncTracker.updateTracker(tracker);
  }

  @Override
  public void updateExpiry(
                           RefData container,
                           long expiry) {
    container.setSovereigntyExpiry(expiry);
    RefCachedData.updateData(container);
  }

  @Override
  public boolean commit(
                        long time,
                        RefSyncTracker tracker,
                        RefData container,
                        RefCachedData item) {

    if (item instanceof Sovereignty) {
      Sovereignty api = (Sovereignty) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        Sovereignty existing = Sovereignty.get(time, api.getSolarSystemID());

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
    return ((IMapAPI) serverRequest).requestSovereignty();
  }

  @Override
  protected long processServerData(
                                   long time,
                                   IResponse serverRequest,
                                   Object data,
                                   List<RefCachedData> updates)
    throws IOException {
    ISovereignty sovList = (ISovereignty) data;
    // Handle sov kill list
    Set<Integer> seenSov = new HashSet<>();
    for (ISystemSovereignty nextSov : sovList.getSystemSovereignty()) {
      updates.add(new Sovereignty(
          nextSov.getAllianceID(), nextSov.getCorporationID(), nextSov.getFactionID(), nextSov.getSolarSystemID(), nextSov.getSolarSystemName()));
      seenSov.add(nextSov.getSolarSystemID());
    }
    // Look for systems which no longer exist and mark for deletion
    AttributeSelector ats = makeAtSelector(time);
    List<Sovereignty> nextBatch = Sovereignty.accessQuery(-1, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
    while (!nextBatch.isEmpty()) {
      for (Sovereignty n : nextBatch) {
        if (!seenSov.contains(n.getSolarSystemID())) {
          n.evolve(null, time);
          updates.add(n);
        }
      }
      nextBatch = Sovereignty.accessQuery(nextBatch.get(nextBatch.size() - 1).getCid(), 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                          ANY_SELECTOR, ANY_SELECTOR);
    }
    return serverRequest.getCachedUntil().getTime();
  }

  private static final SovereigntySync syncher = new SovereigntySync();

  public static SyncStatus sync(
                                long time,
                                RefSynchronizerUtil syncUtil,
                                IResponse serverRequest) {
    return syncher.syncData(time, syncUtil, serverRequest, "Sovereignty");
  }

  public static SyncStatus exclude(
                                   RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "Sovereignty", SyncTracker.SyncState.SYNC_ERROR);
  }

  public static SyncStatus notAllowed(
                                      RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "Sovereignty", SyncTracker.SyncState.NOT_ALLOWED);
  }

}
