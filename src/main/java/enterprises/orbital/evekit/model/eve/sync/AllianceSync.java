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
import enterprises.orbital.evekit.model.eve.Alliance;
import enterprises.orbital.evekit.model.eve.AllianceMemberCorporation;
import enterprises.orbital.evexmlapi.IResponse;
import enterprises.orbital.evexmlapi.eve.IAlliance;
import enterprises.orbital.evexmlapi.eve.IEveAPI;
import enterprises.orbital.evexmlapi.eve.IMemberCorporation;

public class AllianceSync extends AbstractRefSync {
  protected static final Logger log = Logger.getLogger(AllianceSync.class.getName());

  @Override
  public boolean isRefreshed(
                             RefSyncTracker tracker) {
    return tracker.getAllianceListStatus() != SyncTracker.SyncState.NOT_PROCESSED;
  }

  @Override
  public long getExpiryTime(
                            RefData container) {
    return container.getAllianceListExpiry();
  }

  @Override
  public void updateStatus(
                           RefSyncTracker tracker,
                           SyncTracker.SyncState status,
                           String detail) {
    tracker.setAllianceListStatus(status);
    tracker.setAllianceListDetail(detail);
    RefSyncTracker.updateTracker(tracker);
  }

  @Override
  public void updateExpiry(
                           RefData container,
                           long expiry) {
    container.setAllianceListExpiry(expiry);
    RefCachedData.updateData(container);
  }

  @Override
  public boolean commit(
                        long time,
                        RefSyncTracker tracker,
                        RefData container,
                        RefCachedData item) {

    if (item instanceof Alliance) {
      Alliance api = (Alliance) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        Alliance existing = Alliance.get(time, api.getAllianceID());

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
    } else if (item instanceof AllianceMemberCorporation) {
      AllianceMemberCorporation api = (AllianceMemberCorporation) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        AllianceMemberCorporation existing = AllianceMemberCorporation.get(time, api.getAllianceID(), api.getCorporationID());

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
    return ((IEveAPI) serverRequest).requestAlliances();
  }

  @Override
  protected long processServerData(
                                   long time,
                                   IResponse serverRequest,
                                   Object data,
                                   List<RefCachedData> updates)
    throws IOException {
    @SuppressWarnings("unchecked")
    Collection<IAlliance> allianceList = (Collection<IAlliance>) data;
    // Handle alliances
    Set<Long> seenAlliances = new HashSet<>();
    for (IAlliance nextAlliance : allianceList) {
      updates.add(new Alliance(
          nextAlliance.getAllianceID(), nextAlliance.getExecutorCorpID(), nextAlliance.getMemberCount(), nextAlliance.getName(), nextAlliance.getShortName(),
          nextAlliance.getStartDate().getTime()));
      seenAlliances.add(nextAlliance.getAllianceID());
    }
    // Look for alliances which no longer exist and mark for deletion
    AttributeSelector ats = makeAtSelector(time);
    List<Alliance> nextBatch = Alliance.accessQuery(-1, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
    while (!nextBatch.isEmpty()) {
      for (Alliance n : nextBatch) {
        if (!seenAlliances.contains(n.getAllianceID())) {
          n.evolve(null, time);
          updates.add(n);
        }
      }
      nextBatch = Alliance.accessQuery(nextBatch.get(nextBatch.size() - 1).getCid(), 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                       ANY_SELECTOR, ANY_SELECTOR);
    }
    // Handle alliance members
    Map<Long, Set<Long>> seenMembers = new HashMap<>();
    for (IAlliance nextAlliance : allianceList) {
      for (IMemberCorporation nextCorp : nextAlliance.getMemberCorporations()) {
        updates.add(new AllianceMemberCorporation(nextAlliance.getAllianceID(), nextCorp.getCorporationID(), nextCorp.getStartDate().getTime()));
        if (!seenMembers.containsKey(nextAlliance.getAllianceID())) seenMembers.put(nextAlliance.getAllianceID(), new HashSet<>());
        seenMembers.get(nextAlliance.getAllianceID()).add(nextCorp.getCorporationID());
      }
    }
    // Look for members which no longer exist and mark for deletion
    List<AllianceMemberCorporation> nextCorpBatch = AllianceMemberCorporation.accessQuery(-1, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
    while (!nextCorpBatch.isEmpty()) {
      for (AllianceMemberCorporation n : nextCorpBatch) {
        if (!seenMembers.containsKey(n.getAllianceID()) || !seenMembers.get(n.getAllianceID()).contains(n.getCorporationID())) {
          n.evolve(null, time);
          updates.add(n);
        }
      }
      nextCorpBatch = AllianceMemberCorporation.accessQuery(nextCorpBatch.get(nextCorpBatch.size() - 1).getCid(), 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR,
                                                            ANY_SELECTOR);
    }
    return serverRequest.getCachedUntil().getTime();
  }

  private static final AllianceSync syncher = new AllianceSync();

  public static SyncStatus sync(
                                long time,
                                RefSynchronizerUtil syncUtil,
                                IResponse serverRequest) {
    return syncher.syncData(time, syncUtil, serverRequest, "AllianceList");
  }

  public static SyncStatus exclude(
                                   RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "AllianceList", SyncTracker.SyncState.SYNC_ERROR);
  }

  public static SyncStatus notAllowed(
                                      RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "AllianceList", SyncTracker.SyncState.NOT_ALLOWED);
  }

}
