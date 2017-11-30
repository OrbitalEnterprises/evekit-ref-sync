package enterprises.orbital.evekit.model.map.sync;

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
import enterprises.orbital.evekit.model.map.FactionWarSystem;
import enterprises.orbital.evexmlapi.IResponse;
import enterprises.orbital.evexmlapi.map.IFacWarSystem;
import enterprises.orbital.evexmlapi.map.IMapAPI;

public class FacWarSystemsSync extends AbstractRefSync {
  protected static final Logger log = Logger.getLogger(FacWarSystemsSync.class.getName());

  @Override
  public boolean isRefreshed(
                             RefSyncTracker tracker) {
    return tracker.getFacWarSystemsStatus() != SyncTracker.SyncState.NOT_PROCESSED;
  }

  @Override
  public long getExpiryTime(
                            RefData container) {
    return container.getFacWarSystemsExpiry();
  }

  @Override
  public void updateStatus(
                           RefSyncTracker tracker,
                           SyncTracker.SyncState status,
                           String detail) {
    tracker.setFacWarSystemsStatus(status);
    tracker.setFacWarSystemsDetail(detail);
    RefSyncTracker.updateTracker(tracker);
  }

  @Override
  public void updateExpiry(
                           RefData container,
                           long expiry) {
    container.setFacWarSystemsExpiry(expiry);
    RefCachedData.updateData(container);
  }

  @Override
  public boolean commit(
                        long time,
                        RefSyncTracker tracker,
                        RefData container,
                        RefCachedData item) {

    if (item instanceof FactionWarSystem) {
      FactionWarSystem api = (FactionWarSystem) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        FactionWarSystem existing = FactionWarSystem.get(time, api.getSolarSystemID());

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
    return ((IMapAPI) serverRequest).requestFacWarSystems();
  }

  @Override
  protected long processServerData(
                                   long time,
                                   IResponse serverRequest,
                                   Object data,
                                   List<RefCachedData> updates)
    throws IOException {
    @SuppressWarnings("unchecked")
    Collection<IFacWarSystem> systemList = (Collection<IFacWarSystem>) data;
    // Handle error list
    Set<Integer> seenSystems = new HashSet<>();
    for (IFacWarSystem nextSystem : systemList) {
      updates.add(new FactionWarSystem(
          nextSystem.getOccupyingFactionID(), nextSystem.getOccupyingFactionName(), nextSystem.getOwningFactionID(), nextSystem.getOwningFactionName(),
          nextSystem.getSolarSystemID(), nextSystem.getSolarSystemName(), nextSystem.isContested()));
      seenSystems.add(nextSystem.getSolarSystemID());
    }
    // Look for systems which no longer exist and mark for deletion
    AttributeSelector ats = makeAtSelector(time);
    List<FactionWarSystem> nextBatch = FactionWarSystem.accessQuery(-1, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                                                    ANY_SELECTOR, ANY_SELECTOR);
    while (!nextBatch.isEmpty()) {
      for (FactionWarSystem n : nextBatch) {
        if (!seenSystems.contains(n.getSolarSystemID())) {
          n.evolve(null, time);
          updates.add(n);
        }
      }
      nextBatch = FactionWarSystem.accessQuery(nextBatch.get(nextBatch.size() - 1).getCid(), 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                               ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
    }
    return serverRequest.getCachedUntil().getTime();
  }

  private static final FacWarSystemsSync syncher = new FacWarSystemsSync();

  public static SyncStatus sync(
                                long time,
                                RefSynchronizerUtil syncUtil,
                                IResponse serverRequest) {
    return syncher.syncData(time, syncUtil, serverRequest, "FacWarSystems");
  }

  public static SyncStatus exclude(
                                   RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "FacWarSystems", SyncTracker.SyncState.SYNC_ERROR);
  }

  public static SyncStatus notAllowed(
                                      RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "FacWarSystems", SyncTracker.SyncState.NOT_ALLOWED);
  }

}
