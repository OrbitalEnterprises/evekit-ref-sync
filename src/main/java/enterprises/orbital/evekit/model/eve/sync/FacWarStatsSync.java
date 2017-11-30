package enterprises.orbital.evekit.model.eve.sync;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import enterprises.orbital.evekit.model.eve.FactionStats;
import enterprises.orbital.evekit.model.eve.FactionWar;
import enterprises.orbital.evekit.model.eve.FactionWarSummary;
import enterprises.orbital.evexmlapi.IResponse;
import enterprises.orbital.evexmlapi.eve.IEveAPI;
import enterprises.orbital.evexmlapi.eve.IFacWarSummary;
import enterprises.orbital.evexmlapi.eve.IFactionStats;
import enterprises.orbital.evexmlapi.eve.IFactionWar;

public class FacWarStatsSync extends AbstractRefSync {
  protected static final Logger log = Logger.getLogger(FacWarStatsSync.class.getName());

  @Override
  public boolean isRefreshed(
                             RefSyncTracker tracker) {
    return tracker.getFacWarStatsStatus() != SyncTracker.SyncState.NOT_PROCESSED;
  }

  @Override
  public long getExpiryTime(
                            RefData container) {
    return container.getFacWarStatsExpiry();
  }

  @Override
  public void updateStatus(
                           RefSyncTracker tracker,
                           SyncTracker.SyncState status,
                           String detail) {
    tracker.setFacWarStatsStatus(status);
    tracker.setFacWarStatsDetail(detail);
    RefSyncTracker.updateTracker(tracker);
  }

  @Override
  public void updateExpiry(
                           RefData container,
                           long expiry) {
    container.setFacWarStatsExpiry(expiry);
    RefCachedData.updateData(container);
  }

  @Override
  public boolean commit(
                        long time,
                        RefSyncTracker tracker,
                        RefData container,
                        RefCachedData item) {

    if (item instanceof FactionWarSummary) {
      FactionWarSummary api = (FactionWarSummary) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        FactionWarSummary existing = FactionWarSummary.get(time);

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
    } else if (item instanceof FactionStats) {
      FactionStats api = (FactionStats) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        FactionStats existing = FactionStats.get(time, api.getFactionID());

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
    } else if (item instanceof FactionWar) {
      FactionWar api = (FactionWar) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        FactionWar existing = FactionWar.get(time, api.getAgainstID(), api.getFactionID());

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
    return ((IEveAPI) serverRequest).requestFacWarStats();
  }

  @Override
  protected long processServerData(
                                   long time,
                                   IResponse serverRequest,
                                   Object data,
                                   List<RefCachedData> updates)
    throws IOException {
    // Handle summary
    IFacWarSummary summary = (IFacWarSummary) data;
    updates.add(new FactionWarSummary(
        summary.getKillsLastWeek(), summary.getKillsTotal(), summary.getKillsYesterday(), summary.getVictoryPointsLastWeek(), summary.getVictoryPointsTotal(),
        summary.getVictoryPointsYesterday()));
    // Handle stats
    Collection<IFactionStats> factionList = summary.getFactions();
    Set<Long> seenFactions = new HashSet<>();
    for (IFactionStats nextAlliance : factionList) {
      updates.add(new FactionStats(
          nextAlliance.getFactionID(), nextAlliance.getFactionName(), nextAlliance.getKillsLastWeek(), nextAlliance.getKillsTotal(),
          nextAlliance.getKillsYesterday(), nextAlliance.getPilots(), nextAlliance.getSystemsControlled(), nextAlliance.getVictoryPointsLastWeek(),
          nextAlliance.getVictoryPointsTotal(), nextAlliance.getVictoryPointsYesterday()));
      seenFactions.add(nextAlliance.getFactionID());
    }
    // Look for stats which no longer exist and mark for deletion
    AttributeSelector ats = makeAtSelector(time);
    List<FactionStats> nextBatch = FactionStats.accessQuery(-1, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                                            ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
    while (!nextBatch.isEmpty()) {
      for (FactionStats n : nextBatch) {
        if (!seenFactions.contains(n.getFactionID())) {
          n.evolve(null, time);
          updates.add(n);
        }
      }
      nextBatch = FactionStats.accessQuery(nextBatch.get(nextBatch.size() - 1).getCid(), 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                           ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
    }
    // Handle wars
    Map<Long, Set<Long>> seenWars = new HashMap<>();
    for (IFactionWar nextWar : summary.getWars()) {
      updates.add(new FactionWar(nextWar.getAgainstID(), nextWar.getAgainstName(), nextWar.getFactionID(), nextWar.getFactionName()));
      if (!seenWars.containsKey(nextWar.getAgainstID())) seenWars.put(nextWar.getAgainstID(), new HashSet<>());
      seenWars.get(nextWar.getAgainstID()).add(nextWar.getFactionID());
    }
    // Look for war which no longer exist and mark for deletion
    List<FactionWar> nextWarBatch = FactionWar.accessQuery(-1, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
    while (!nextWarBatch.isEmpty()) {
      for (FactionWar n : nextWarBatch) {
        if (!seenWars.containsKey(n.getAgainstID()) || !seenWars.get(n.getAgainstID()).contains(n.getFactionID())) {
          n.evolve(null, time);
          updates.add(n);
        }
      }
      nextWarBatch = FactionWar.accessQuery(nextWarBatch.get(nextWarBatch.size() - 1).getCid(), 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                            ANY_SELECTOR);
    }
    return serverRequest.getCachedUntil().getTime();
  }

  private static final FacWarStatsSync syncher = new FacWarStatsSync();

  public static SyncStatus sync(
                                long time,
                                RefSynchronizerUtil syncUtil,
                                IResponse serverRequest) {
    return syncher.syncData(time, syncUtil, serverRequest, "FacWarStats");
  }

  public static SyncStatus exclude(
                                   RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "FacWarStats", SyncTracker.SyncState.SYNC_ERROR);
  }

  public static SyncStatus notAllowed(
                                      RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "FacWarStats", SyncTracker.SyncState.NOT_ALLOWED);
  }

}
