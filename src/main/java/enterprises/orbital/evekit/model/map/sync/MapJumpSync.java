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
import enterprises.orbital.evekit.model.map.MapJump;
import enterprises.orbital.evexmlapi.IResponse;
import enterprises.orbital.evexmlapi.map.IJump;
import enterprises.orbital.evexmlapi.map.IMapAPI;
import enterprises.orbital.evexmlapi.map.IMapJump;

public class MapJumpSync extends AbstractRefSync {
  protected static final Logger log = Logger.getLogger(MapJumpSync.class.getName());

  @Override
  public boolean isRefreshed(
                             RefSyncTracker tracker) {
    return tracker.getMapJumpStatus() != SyncTracker.SyncState.NOT_PROCESSED;
  }

  @Override
  public long getExpiryTime(
                            RefData container) {
    return container.getMapJumpExpiry();
  }

  @Override
  public void updateStatus(
                           RefSyncTracker tracker,
                           SyncTracker.SyncState status,
                           String detail) {
    tracker.setMapJumpStatus(status);
    tracker.setMapJumpDetail(detail);
    RefSyncTracker.updateTracker(tracker);
  }

  @Override
  public void updateExpiry(
                           RefData container,
                           long expiry) {
    container.setMapJumpExpiry(expiry);
    RefCachedData.updateData(container);
  }

  @Override
  public boolean commit(
                        long time,
                        RefSyncTracker tracker,
                        RefData container,
                        RefCachedData item) {

    if (item instanceof MapJump) {
      MapJump api = (MapJump) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        MapJump existing = MapJump.get(time, api.getSolarSystemID());

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
    return ((IMapAPI) serverRequest).requestJumps();
  }

  @Override
  protected long processServerData(
                                   long time,
                                   IResponse serverRequest,
                                   Object data,
                                   List<RefCachedData> updates)
    throws IOException {
    IMapJump jumpList = (IMapJump) data;
    // Handle map jump list
    Set<Integer> seenJumps = new HashSet<>();
    for (IJump nextJump : jumpList.getJumps()) {
      updates.add(new MapJump(nextJump.getSolarSystemID(), nextJump.getShipJumps()));
      seenJumps.add(nextJump.getSolarSystemID());
    }
    // Look for systems which no longer exist and mark for deletion
    AttributeSelector ats = makeAtSelector(time);
    List<MapJump> nextBatch = MapJump.accessQuery(-1, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
    while (!nextBatch.isEmpty()) {
      for (MapJump n : nextBatch) {
        if (!seenJumps.contains(n.getSolarSystemID())) {
          n.evolve(null, time);
          updates.add(n);
        }
      }
      nextBatch = MapJump.accessQuery(nextBatch.get(nextBatch.size() - 1).getCid(), 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
    }
    return serverRequest.getCachedUntil().getTime();
  }

  private static final MapJumpSync syncher = new MapJumpSync();

  public static SyncStatus sync(
                                long time,
                                RefSynchronizerUtil syncUtil,
                                IResponse serverRequest) {
    return syncher.syncData(time, syncUtil, serverRequest, "MapJump");
  }

  public static SyncStatus exclude(
                                   RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "MapJump", SyncTracker.SyncState.SYNC_ERROR);
  }

  public static SyncStatus notAllowed(
                                      RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "MapJump", SyncTracker.SyncState.NOT_ALLOWED);
  }

}
