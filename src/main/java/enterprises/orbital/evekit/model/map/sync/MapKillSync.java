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
import enterprises.orbital.evekit.model.map.MapKill;
import enterprises.orbital.evexmlapi.IResponse;
import enterprises.orbital.evexmlapi.map.IMapAPI;
import enterprises.orbital.evexmlapi.map.IMapKill;
import enterprises.orbital.evexmlapi.map.ISystemKills;

public class MapKillSync extends AbstractRefSync {
  protected static final Logger log = Logger.getLogger(MapKillSync.class.getName());

  @Override
  public boolean isRefreshed(
                             RefSyncTracker tracker) {
    return tracker.getMapKillStatus() != SyncTracker.SyncState.NOT_PROCESSED;
  }

  @Override
  public long getExpiryTime(
                            RefData container) {
    return container.getMapKillExpiry();
  }

  @Override
  public void updateStatus(
                           RefSyncTracker tracker,
                           SyncTracker.SyncState status,
                           String detail) {
    tracker.setMapKillStatus(status);
    tracker.setMapKillDetail(detail);
    RefSyncTracker.updateTracker(tracker);
  }

  @Override
  public void updateExpiry(
                           RefData container,
                           long expiry) {
    container.setMapKillExpiry(expiry);
    RefCachedData.updateData(container);
  }

  @Override
  public boolean commit(
                        long time,
                        RefSyncTracker tracker,
                        RefData container,
                        RefCachedData item) {

    if (item instanceof MapKill) {
      MapKill api = (MapKill) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        MapKill existing = MapKill.get(time, api.getSolarSystemID());

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
    return ((IMapAPI) serverRequest).requestKills();
  }

  @Override
  protected long processServerData(
                                   long time,
                                   IResponse serverRequest,
                                   Object data,
                                   List<RefCachedData> updates)
    throws IOException {
    IMapKill killList = (IMapKill) data;
    // Handle map kill list
    Set<Integer> seenKills = new HashSet<>();
    for (ISystemKills nextKill : killList.getKills()) {
      updates.add(new MapKill(nextKill.getFactionKills(), nextKill.getPodKills(), nextKill.getShipKills(), nextKill.getSolarSystemID()));
      seenKills.add(nextKill.getSolarSystemID());
    }
    // Look for systems which no longer exist and mark for deletion
    AttributeSelector ats = makeAtSelector(time);
    List<MapKill> nextBatch = MapKill.accessQuery(-1, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
    while (!nextBatch.isEmpty()) {
      for (MapKill n : nextBatch) {
        if (!seenKills.contains(n.getSolarSystemID())) {
          n.evolve(null, time);
          updates.add(n);
        }
      }
      nextBatch = MapKill.accessQuery(nextBatch.get(nextBatch.size() - 1).getCid(), 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
    }
    return serverRequest.getCachedUntil().getTime();
  }

  private static final MapKillSync syncher = new MapKillSync();

  public static SyncStatus sync(
                                long time,
                                RefSynchronizerUtil syncUtil,
                                IResponse serverRequest) {
    return syncher.syncData(time, syncUtil, serverRequest, "MapKill");
  }

  public static SyncStatus exclude(
                                   RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "MapKill", SyncTracker.SyncState.SYNC_ERROR);
  }

  public static SyncStatus notAllowed(
                                      RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "MapKill", SyncTracker.SyncState.NOT_ALLOWED);
  }

}
