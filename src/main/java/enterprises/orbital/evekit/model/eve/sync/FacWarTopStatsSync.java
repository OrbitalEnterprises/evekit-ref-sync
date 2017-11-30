package enterprises.orbital.evekit.model.eve.sync;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
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
import enterprises.orbital.evekit.model.eve.CharacterKillStat;
import enterprises.orbital.evekit.model.eve.CharacterVictoryPointStat;
import enterprises.orbital.evekit.model.eve.CorporationKillStat;
import enterprises.orbital.evekit.model.eve.CorporationVictoryPointStat;
import enterprises.orbital.evekit.model.eve.FactionKillStat;
import enterprises.orbital.evekit.model.eve.FactionVictoryPointStat;
import enterprises.orbital.evekit.model.eve.StatAttribute;
import enterprises.orbital.evexmlapi.IResponse;
import enterprises.orbital.evexmlapi.eve.ICharacterKillStat;
import enterprises.orbital.evexmlapi.eve.ICharacterVictoryPointStat;
import enterprises.orbital.evexmlapi.eve.ICorporationKillStat;
import enterprises.orbital.evexmlapi.eve.ICorporationVictoryPointStat;
import enterprises.orbital.evexmlapi.eve.IEveAPI;
import enterprises.orbital.evexmlapi.eve.IFacWarTopStats;
import enterprises.orbital.evexmlapi.eve.IFacWarTopSummary;
import enterprises.orbital.evexmlapi.eve.IFactionKillStat;
import enterprises.orbital.evexmlapi.eve.IFactionVictoryPointStat;

public class FacWarTopStatsSync extends AbstractRefSync {
  protected static final Logger log = Logger.getLogger(FacWarTopStatsSync.class.getName());

  @Override
  public boolean isRefreshed(
                             RefSyncTracker tracker) {
    return tracker.getFacWarTopStatsStatus() != SyncTracker.SyncState.NOT_PROCESSED;
  }

  @Override
  public long getExpiryTime(
                            RefData container) {
    return container.getFacWarTopStatsExpiry();
  }

  @Override
  public void updateStatus(
                           RefSyncTracker tracker,
                           SyncTracker.SyncState status,
                           String detail) {
    tracker.setFacWarTopStatsStatus(status);
    tracker.setFacWarTopStatsDetail(detail);
    RefSyncTracker.updateTracker(tracker);
  }

  @Override
  public void updateExpiry(
                           RefData container,
                           long expiry) {
    container.setFacWarTopStatsExpiry(expiry);
    RefCachedData.updateData(container);
  }

  @Override
  public boolean commit(
                        long time,
                        RefSyncTracker tracker,
                        RefData container,
                        RefCachedData item) {

    if (item instanceof CharacterKillStat) {
      CharacterKillStat api = (CharacterKillStat) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        CharacterKillStat existing = CharacterKillStat.get(time, api.getAttribute(), api.getCharacterID());

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
    } else if (item instanceof CharacterVictoryPointStat) {
      CharacterVictoryPointStat api = (CharacterVictoryPointStat) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        CharacterVictoryPointStat existing = CharacterVictoryPointStat.get(time, api.getAttribute(), api.getCharacterID());

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
    } else if (item instanceof CorporationKillStat) {
      CorporationKillStat api = (CorporationKillStat) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        CorporationKillStat existing = CorporationKillStat.get(time, api.getAttribute(), api.getCorporationID());

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
    } else if (item instanceof CorporationVictoryPointStat) {
      CorporationVictoryPointStat api = (CorporationVictoryPointStat) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        CorporationVictoryPointStat existing = CorporationVictoryPointStat.get(time, api.getAttribute(), api.getCorporationID());

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
    } else if (item instanceof FactionKillStat) {
      FactionKillStat api = (FactionKillStat) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        FactionKillStat existing = FactionKillStat.get(time, api.getAttribute(), api.getFactionID());

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
    } else if (item instanceof FactionVictoryPointStat) {
      FactionVictoryPointStat api = (FactionVictoryPointStat) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        FactionVictoryPointStat existing = FactionVictoryPointStat.get(time, api.getAttribute(), api.getFactionID());

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
    return ((IEveAPI) serverRequest).requestFacWarTopStats();
  }

  private interface StatCreator<A extends RefCachedData, B> {
    public A createStat(
                        StatAttribute attr,
                        B source,
                        Set<Long> seenSet);
  }

  private interface GetExistingData<A extends RefCachedData> {
    public List<A> getNextSet(
                              StatAttribute attr,
                              List<A> previousSet);
  }

  private interface GetItemID<A extends RefCachedData> {
    public long getID(
                      A item);
  }

  private <A extends RefCachedData, B> void populateStat(
                                                         long time,
                                                         StatAttribute attr,
                                                         Collection<B> data,
                                                         List<RefCachedData> updates,
                                                         StatCreator<A, B> ctor,
                                                         GetExistingData<A> setRetriever,
                                                         GetItemID<A> getID) {
    Set<Long> seen = new HashSet<>();
    for (B nextStat : data) {
      updates.add(ctor.createStat(attr, nextStat, seen));
    }
    List<A> nextBatch = setRetriever.getNextSet(attr, Collections.emptyList());
    while (!nextBatch.isEmpty()) {
      for (A n : nextBatch) {
        if (!seen.contains(getID.getID(n))) {
          n.evolve(null, time);
          updates.add(n);
        }
      }
      nextBatch = setRetriever.getNextSet(attr, nextBatch);
    }
  }

  @Override
  protected long processServerData(
                                   long time,
                                   IResponse serverRequest,
                                   Object data,
                                   List<RefCachedData> updates)
    throws IOException {
    // Handle all stats
    final AttributeSelector ats = makeAtSelector(time);
    IFacWarTopSummary summary = (IFacWarTopSummary) data;
    // Handle character stats
    IFacWarTopStats<ICharacterKillStat, ICharacterVictoryPointStat> charStats = summary.getCharacterStats();
    StatCreator<CharacterKillStat, ICharacterKillStat> charKillCreator = new StatCreator<CharacterKillStat, ICharacterKillStat>() {

      @Override
      public CharacterKillStat createStat(
                                          StatAttribute attr,
                                          ICharacterKillStat source,
                                          Set<Long> seenSet) {
        seenSet.add(source.getCharacterID());
        return new CharacterKillStat(attr, source.getKills(), source.getCharacterID(), source.getCharacterName());
      }

    };
    GetItemID<CharacterKillStat> charKillIDGet = new GetItemID<CharacterKillStat>() {

      @Override
      public long getID(
                        CharacterKillStat item) {
        return item.getCharacterID();
      }
    };
    GetExistingData<CharacterKillStat> charKillGetExisting = new GetExistingData<CharacterKillStat>() {

      @Override
      public List<CharacterKillStat> getNextSet(
                                                StatAttribute attr,
                                                List<CharacterKillStat> previousSet) {
        long contid = previousSet.isEmpty() ? -1 : previousSet.get(previousSet.size() - 1).getCid();
        return CharacterKillStat.accessQuery(contid, 1000, false, ats, new AttributeSelector("{values: ['" + attr.toString() + "']}"), ANY_SELECTOR,
                                             ANY_SELECTOR, ANY_SELECTOR);
      }
    };
    populateStat(time, StatAttribute.LAST_WEEK, charStats.getKillsLastWeek(), updates, charKillCreator, charKillGetExisting, charKillIDGet);
    populateStat(time, StatAttribute.TOTAL, charStats.getKillsTotal(), updates, charKillCreator, charKillGetExisting, charKillIDGet);
    populateStat(time, StatAttribute.YESTERDAY, charStats.getKillsYesterday(), updates, charKillCreator, charKillGetExisting, charKillIDGet);
    StatCreator<CharacterVictoryPointStat, ICharacterVictoryPointStat> charVPCreator = new StatCreator<CharacterVictoryPointStat, ICharacterVictoryPointStat>() {

      @Override
      public CharacterVictoryPointStat createStat(
                                                  StatAttribute attr,
                                                  ICharacterVictoryPointStat source,
                                                  Set<Long> seenSet) {
        seenSet.add(source.getCharacterID());
        return new CharacterVictoryPointStat(attr, source.getVictoryPoints(), source.getCharacterID(), source.getCharacterName());
      }

    };
    GetItemID<CharacterVictoryPointStat> charVPIDGet = new GetItemID<CharacterVictoryPointStat>() {

      @Override
      public long getID(
                        CharacterVictoryPointStat item) {
        return item.getCharacterID();
      }
    };
    GetExistingData<CharacterVictoryPointStat> charVPGetExisting = new GetExistingData<CharacterVictoryPointStat>() {

      @Override
      public List<CharacterVictoryPointStat> getNextSet(
                                                        StatAttribute attr,
                                                        List<CharacterVictoryPointStat> previousSet) {
        long contid = previousSet.isEmpty() ? -1 : previousSet.get(previousSet.size() - 1).getCid();
        return CharacterVictoryPointStat.accessQuery(contid, 1000, false, ats, new AttributeSelector("{values: ['" + attr.toString() + "']}"), ANY_SELECTOR,
                                                     ANY_SELECTOR, ANY_SELECTOR);
      }
    };
    populateStat(time, StatAttribute.LAST_WEEK, charStats.getVictoryPointsLastWeek(), updates, charVPCreator, charVPGetExisting, charVPIDGet);
    populateStat(time, StatAttribute.TOTAL, charStats.getVictoryPointsTotal(), updates, charVPCreator, charVPGetExisting, charVPIDGet);
    populateStat(time, StatAttribute.YESTERDAY, charStats.getVictoryPointsYesterday(), updates, charVPCreator, charVPGetExisting, charVPIDGet);
    // Handle corporation stats
    IFacWarTopStats<ICorporationKillStat, ICorporationVictoryPointStat> corpStats = summary.getCorporationStats();
    StatCreator<CorporationKillStat, ICorporationKillStat> corpKillCreator = new StatCreator<CorporationKillStat, ICorporationKillStat>() {

      @Override
      public CorporationKillStat createStat(
                                            StatAttribute attr,
                                            ICorporationKillStat source,
                                            Set<Long> seenSet) {
        seenSet.add(source.getCorporationID());
        return new CorporationKillStat(attr, source.getKills(), source.getCorporationID(), source.getCorporationName());
      }

    };
    GetItemID<CorporationKillStat> corpKillIDGet = new GetItemID<CorporationKillStat>() {

      @Override
      public long getID(
                        CorporationKillStat item) {
        return item.getCorporationID();
      }
    };
    GetExistingData<CorporationKillStat> corpKillGetExisting = new GetExistingData<CorporationKillStat>() {

      @Override
      public List<CorporationKillStat> getNextSet(
                                                  StatAttribute attr,
                                                  List<CorporationKillStat> previousSet) {
        long contid = previousSet.isEmpty() ? -1 : previousSet.get(previousSet.size() - 1).getCid();
        return CorporationKillStat.accessQuery(contid, 1000, false, ats, new AttributeSelector("{values: ['" + attr.toString() + "']}"), ANY_SELECTOR,
                                               ANY_SELECTOR, ANY_SELECTOR);
      }
    };
    populateStat(time, StatAttribute.LAST_WEEK, corpStats.getKillsLastWeek(), updates, corpKillCreator, corpKillGetExisting, corpKillIDGet);
    populateStat(time, StatAttribute.TOTAL, corpStats.getKillsTotal(), updates, corpKillCreator, corpKillGetExisting, corpKillIDGet);
    populateStat(time, StatAttribute.YESTERDAY, corpStats.getKillsYesterday(), updates, corpKillCreator, corpKillGetExisting, corpKillIDGet);
    StatCreator<CorporationVictoryPointStat, ICorporationVictoryPointStat> corpVPCreator = new StatCreator<CorporationVictoryPointStat, ICorporationVictoryPointStat>() {

      @Override
      public CorporationVictoryPointStat createStat(
                                                    StatAttribute attr,
                                                    ICorporationVictoryPointStat source,
                                                    Set<Long> seenSet) {
        seenSet.add(source.getCorporationID());
        return new CorporationVictoryPointStat(attr, source.getVictoryPoints(), source.getCorporationID(), source.getCorporationName());
      }

    };
    GetItemID<CorporationVictoryPointStat> corpVPIDGet = new GetItemID<CorporationVictoryPointStat>() {

      @Override
      public long getID(
                        CorporationVictoryPointStat item) {
        return item.getCorporationID();
      }
    };
    GetExistingData<CorporationVictoryPointStat> corpVPGetExisting = new GetExistingData<CorporationVictoryPointStat>() {

      @Override
      public List<CorporationVictoryPointStat> getNextSet(
                                                          StatAttribute attr,
                                                          List<CorporationVictoryPointStat> previousSet) {
        long contid = previousSet.isEmpty() ? -1 : previousSet.get(previousSet.size() - 1).getCid();
        return CorporationVictoryPointStat.accessQuery(contid, 1000, false, ats, new AttributeSelector("{values: ['" + attr.toString() + "']}"), ANY_SELECTOR,
                                                       ANY_SELECTOR, ANY_SELECTOR);
      }
    };
    populateStat(time, StatAttribute.LAST_WEEK, corpStats.getVictoryPointsLastWeek(), updates, corpVPCreator, corpVPGetExisting, corpVPIDGet);
    populateStat(time, StatAttribute.TOTAL, corpStats.getVictoryPointsTotal(), updates, corpVPCreator, corpVPGetExisting, corpVPIDGet);
    populateStat(time, StatAttribute.YESTERDAY, corpStats.getVictoryPointsYesterday(), updates, corpVPCreator, corpVPGetExisting, corpVPIDGet);
    // Handle faction stats
    IFacWarTopStats<IFactionKillStat, IFactionVictoryPointStat> factionStats = summary.getFactionStats();
    StatCreator<FactionKillStat, IFactionKillStat> factionKillCreator = new StatCreator<FactionKillStat, IFactionKillStat>() {

      @Override
      public FactionKillStat createStat(
                                        StatAttribute attr,
                                        IFactionKillStat source,
                                        Set<Long> seenSet) {
        seenSet.add(source.getFactionID());
        return new FactionKillStat(attr, source.getKills(), source.getFactionID(), source.getFactionName());
      }

    };
    GetItemID<FactionKillStat> factionKillIDGet = new GetItemID<FactionKillStat>() {

      @Override
      public long getID(
                        FactionKillStat item) {
        return item.getFactionID();
      }
    };
    GetExistingData<FactionKillStat> factionKillGetExisting = new GetExistingData<FactionKillStat>() {

      @Override
      public List<FactionKillStat> getNextSet(
                                              StatAttribute attr,
                                              List<FactionKillStat> previousSet) {
        long contid = previousSet.isEmpty() ? -1 : previousSet.get(previousSet.size() - 1).getCid();
        return FactionKillStat.accessQuery(contid, 1000, false, ats, new AttributeSelector("{values: ['" + attr.toString() + "']}"), ANY_SELECTOR, ANY_SELECTOR,
                                           ANY_SELECTOR);
      }
    };
    populateStat(time, StatAttribute.LAST_WEEK, factionStats.getKillsLastWeek(), updates, factionKillCreator, factionKillGetExisting, factionKillIDGet);
    populateStat(time, StatAttribute.TOTAL, factionStats.getKillsTotal(), updates, factionKillCreator, factionKillGetExisting, factionKillIDGet);
    populateStat(time, StatAttribute.YESTERDAY, factionStats.getKillsYesterday(), updates, factionKillCreator, factionKillGetExisting, factionKillIDGet);
    StatCreator<FactionVictoryPointStat, IFactionVictoryPointStat> factionVPCreator = new StatCreator<FactionVictoryPointStat, IFactionVictoryPointStat>() {

      @Override
      public FactionVictoryPointStat createStat(
                                                StatAttribute attr,
                                                IFactionVictoryPointStat source,
                                                Set<Long> seenSet) {
        seenSet.add(source.getFactionID());
        return new FactionVictoryPointStat(attr, source.getVictoryPoints(), source.getFactionID(), source.getFactionName());
      }

    };
    GetItemID<FactionVictoryPointStat> factionVPIDGet = new GetItemID<FactionVictoryPointStat>() {

      @Override
      public long getID(
                        FactionVictoryPointStat item) {
        return item.getFactionID();
      }
    };
    GetExistingData<FactionVictoryPointStat> factionVPGetExisting = new GetExistingData<FactionVictoryPointStat>() {

      @Override
      public List<FactionVictoryPointStat> getNextSet(
                                                      StatAttribute attr,
                                                      List<FactionVictoryPointStat> previousSet) {
        long contid = previousSet.isEmpty() ? -1 : previousSet.get(previousSet.size() - 1).getCid();
        return FactionVictoryPointStat.accessQuery(contid, 1000, false, ats, new AttributeSelector("{values: ['" + attr.toString() + "']}"), ANY_SELECTOR,
                                                   ANY_SELECTOR, ANY_SELECTOR);
      }
    };
    populateStat(time, StatAttribute.LAST_WEEK, factionStats.getVictoryPointsLastWeek(), updates, factionVPCreator, factionVPGetExisting, factionVPIDGet);
    populateStat(time, StatAttribute.TOTAL, factionStats.getVictoryPointsTotal(), updates, factionVPCreator, factionVPGetExisting, factionVPIDGet);
    populateStat(time, StatAttribute.YESTERDAY, factionStats.getVictoryPointsYesterday(), updates, factionVPCreator, factionVPGetExisting, factionVPIDGet);

    // Return cache time
    return serverRequest.getCachedUntil().getTime();
  }

  private static final FacWarTopStatsSync syncher = new FacWarTopStatsSync();

  public static SyncStatus sync(
                                long time,
                                RefSynchronizerUtil syncUtil,
                                IResponse serverRequest) {
    return syncher.syncData(time, syncUtil, serverRequest, "FacWarTopStats");
  }

  public static SyncStatus exclude(
                                   RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "FacWarTopStats", SyncTracker.SyncState.SYNC_ERROR);
  }

  public static SyncStatus notAllowed(
                                      RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "FacWarTopStats", SyncTracker.SyncState.NOT_ALLOWED);
  }

}
