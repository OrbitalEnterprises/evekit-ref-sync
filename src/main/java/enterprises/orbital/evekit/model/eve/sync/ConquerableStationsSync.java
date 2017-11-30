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
import enterprises.orbital.evekit.model.eve.ConquerableStation;
import enterprises.orbital.evexmlapi.IResponse;
import enterprises.orbital.evexmlapi.eve.IConquerableStation;
import enterprises.orbital.evexmlapi.eve.IEveAPI;

public class ConquerableStationsSync extends AbstractRefSync {
  protected static final Logger log = Logger.getLogger(ConquerableStationsSync.class.getName());

  @Override
  public boolean isRefreshed(
                             RefSyncTracker tracker) {
    return tracker.getConquerableStationsStatus() != SyncTracker.SyncState.NOT_PROCESSED;
  }

  @Override
  public long getExpiryTime(
                            RefData container) {
    return container.getConquerableStationsExpiry();
  }

  @Override
  public void updateStatus(
                           RefSyncTracker tracker,
                           SyncTracker.SyncState status,
                           String detail) {
    tracker.setConquerableStationsStatus(status);
    tracker.setConquerableStationsDetail(detail);
    RefSyncTracker.updateTracker(tracker);
  }

  @Override
  public void updateExpiry(
                           RefData container,
                           long expiry) {
    container.setConquerableStationsExpiry(expiry);
    RefCachedData.updateData(container);
  }

  @Override
  public boolean commit(
                        long time,
                        RefSyncTracker tracker,
                        RefData container,
                        RefCachedData item) {

    if (item instanceof ConquerableStation) {
      ConquerableStation api = (ConquerableStation) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        ConquerableStation existing = ConquerableStation.get(time, api.getStationID());

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
    return ((IEveAPI) serverRequest).requestConquerableStations();
  }

  @Override
  protected long processServerData(
                                   long time,
                                   IResponse serverRequest,
                                   Object data,
                                   List<RefCachedData> updates)
    throws IOException {
    @SuppressWarnings("unchecked")
    Collection<IConquerableStation> stationList = (Collection<IConquerableStation>) data;
    // Handle stations
    Set<Long> seenStations = new HashSet<>();
    for (IConquerableStation nextStation : stationList) {
      updates.add(new ConquerableStation(
          nextStation.getCorporationID(), nextStation.getCorporationName(), nextStation.getSolarSystemID(), nextStation.getStationID(),
          nextStation.getStationName(), nextStation.getStationTypeID(), nextStation.getX(), nextStation.getY(), nextStation.getZ()));
      seenStations.add(nextStation.getStationID());
    }
    // Look for stations which no longer exist and mark for deletion
    AttributeSelector ats = makeAtSelector(time);
    List<ConquerableStation> nextBatch = ConquerableStation.accessQuery(-1, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                                                        ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
    while (!nextBatch.isEmpty()) {
      for (ConquerableStation n : nextBatch) {
        if (!seenStations.contains(n.getStationID())) {
          n.evolve(null, time);
          updates.add(n);
        }
      }
      nextBatch = ConquerableStation.accessQuery(nextBatch.get(nextBatch.size() - 1).getCid(), 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                                 ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
    }
    return serverRequest.getCachedUntil().getTime();
  }

  private static final ConquerableStationsSync syncher = new ConquerableStationsSync();

  public static SyncStatus sync(
                                long time,
                                RefSynchronizerUtil syncUtil,
                                IResponse serverRequest) {
    return syncher.syncData(time, syncUtil, serverRequest, "ConquerableStations");
  }

  public static SyncStatus exclude(
                                   RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "ConquerableStations", SyncTracker.SyncState.SYNC_ERROR);
  }

  public static SyncStatus notAllowed(
                                      RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "ConquerableStations", SyncTracker.SyncState.NOT_ALLOWED);
  }

}
