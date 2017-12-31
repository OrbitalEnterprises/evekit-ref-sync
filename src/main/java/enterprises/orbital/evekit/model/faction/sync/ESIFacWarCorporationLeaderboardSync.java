package enterprises.orbital.evekit.model.faction.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.FactionWarfareApi;
import enterprises.orbital.eve.esi.client.invoker.ApiException;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.*;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.faction.CorporationKillStat;
import enterprises.orbital.evekit.model.faction.CorporationVictoryPointStat;
import enterprises.orbital.evekit.model.faction.StatAttribute;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ESIFacWarCorporationLeaderboardSync extends AbstractESIRefSync<GetFwLeaderboardsCorporationsOk> {
  protected static final Logger log = Logger.getLogger(ESIFacWarCorporationLeaderboardSync.class.getName());

  @Override
  public ESIRefSyncEndpoint endpoint() {
    return ESIRefSyncEndpoint.REF_FW_CORP_LEADERBOARD;
  }

  @Override
  protected void commit(long time,
                        RefCachedData item) throws IOException {
    assert item instanceof CorporationKillStat || item instanceof CorporationVictoryPointStat;
    RefCachedData existing;
    if (item instanceof CorporationKillStat) {
      CorporationKillStat api = (CorporationKillStat) item;
      existing = api.getLifeStart() == 0 ? CorporationKillStat.get(time, api.getAttribute(), api.getCorporationID()) : null;
    } else {
      CorporationVictoryPointStat api = (CorporationVictoryPointStat) item;
      existing = api.getLifeStart() == 0 ? CorporationVictoryPointStat.get(time, api.getAttribute(), api.getCorporationID()) : null;
    }
    evolveOrAdd(time, existing, item);
  }

  @Override
  protected ESIRefServerResult<GetFwLeaderboardsCorporationsOk> getServerData(
      ESIClientProvider cp) throws ApiException, IOException {
    FactionWarfareApi apiInstance = cp.getFactionWarfareApi();
    ApiResponse<GetFwLeaderboardsCorporationsOk> result = apiInstance.getFwLeaderboardsCorporationsWithHttpInfo(null, null, null);
    checkCommonProblems(result);
    return new ESIRefServerResult<>(extractExpiry(result, OrbitalProperties.getCurrentTime() + maxDelay()), result.getData());
  }

  private interface CorporationGetter<A> {
    Integer getCorporation(A instance);
  }

  private interface AmountGetter<A> {
    Integer getAmount(A instance);
  }

  private <A> List<CorporationKillStat> produceKillStatList(List<A> source, StatAttribute tp, CorporationGetter<A> fg,
                                                            AmountGetter<A> ag) {
    return source.stream()
                 .map((val) -> new CorporationKillStat(tp, ag.getAmount(val), fg.getCorporation(val)))
                 .collect(Collectors.toList());
  }

  private <A> List<CorporationVictoryPointStat> produceVPStatList(List<A> source, StatAttribute tp,
                                                                  CorporationGetter<A> fg,
                                                                  AmountGetter<A> ag) {
    return source.stream()
                 .map((val) -> new CorporationVictoryPointStat(tp, ag.getAmount(val), fg.getCorporation(val)))
                 .collect(Collectors.toList());
  }

  @Override
  protected void processServerData(long time, ESIRefServerResult<GetFwLeaderboardsCorporationsOk> data,
                                   List<RefCachedData> updates) throws IOException {
    GetFwLeaderboardsCorporationsOk serverData = data.getData();
    // Create updates for all entries.
    // Collect kills first
    Set<Pair<StatAttribute, Integer>> seenCorporationKills = new HashSet<>();
    List<CorporationKillStat> allKills = new ArrayList<>();
    allKills.addAll(produceKillStatList(serverData.getKills()
                                                  .getActiveTotal(), StatAttribute.TOTAL,
                                        GetFwLeaderboardsCorporationsActiveTotal::getCorporationId,
                                        GetFwLeaderboardsCorporationsActiveTotal::getAmount));
    allKills.addAll(produceKillStatList(serverData.getKills()
                                                  .getLastWeek(), StatAttribute.LAST_WEEK,
                                        GetFwLeaderboardsCorporationsLastWeek::getCorporationId,
                                        GetFwLeaderboardsCorporationsLastWeek::getAmount));
    allKills.addAll(produceKillStatList(serverData.getKills()
                                                  .getYesterday(), StatAttribute.YESTERDAY,
                                        GetFwLeaderboardsCorporationsYesterday::getCorporationId,
                                        GetFwLeaderboardsCorporationsYesterday::getAmount));
    for (CorporationKillStat next : allKills) {
      seenCorporationKills.add(Pair.of(next.getAttribute(), next.getCorporationID()));
    }
    updates.addAll(allKills);
    // Now collect victory points
    Set<Pair<StatAttribute, Integer>> seenCorporationVPs = new HashSet<>();
    List<CorporationVictoryPointStat> allVPs = new ArrayList<>();
    allVPs.addAll(produceVPStatList(serverData.getVictoryPoints()
                                              .getActiveTotal(), StatAttribute.TOTAL,
                                    GetFwLeaderboardsCorporationsActiveTotal1::getCorporationId,
                                    GetFwLeaderboardsCorporationsActiveTotal1::getAmount));
    allVPs.addAll(produceVPStatList(serverData.getVictoryPoints()
                                              .getLastWeek(), StatAttribute.LAST_WEEK,
                                    GetFwLeaderboardsCorporationsLastWeek1::getCorporationId,
                                    GetFwLeaderboardsCorporationsLastWeek1::getAmount));
    allVPs.addAll(produceVPStatList(serverData.getVictoryPoints()
                                              .getYesterday(), StatAttribute.YESTERDAY,
                                    GetFwLeaderboardsCorporationsYesterday1::getCorporationId,
                                    GetFwLeaderboardsCorporationsYesterday1::getAmount));
    for (CorporationVictoryPointStat next : allVPs) {
      seenCorporationVPs.add(Pair.of(next.getAttribute(), next.getCorporationID()));
    }
    updates.addAll(allVPs);
    // Look for any corporation/attribute pairs not contained in the update and schedule for EOL
    List<CorporationKillStat> storedKills = retrieveAll(time, (long contid, AttributeSelector at) ->
        CorporationKillStat.accessQuery(contid, 1000, false, at, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR));
    for (CorporationKillStat next : storedKills) {
      if (!seenCorporationKills.contains(Pair.of(next.getAttribute(), next.getCorporationID()))) {
        next.evolve(null, time);
        updates.add(next);
      }
    }
    List<CorporationVictoryPointStat> storedVPs = retrieveAll(time, (long contid, AttributeSelector at) ->
        CorporationVictoryPointStat.accessQuery(contid, 1000, false, at, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR));
    for (CorporationVictoryPointStat next : storedVPs) {
      if (!seenCorporationVPs.contains(Pair.of(next.getAttribute(), next.getCorporationID()))) {
        next.evolve(null, time);
        updates.add(next);
      }
    }
  }

  @Override
  public ESIRefEndpointSyncTracker getCurrentTracker() throws IOException, TrackerNotFoundException {
    return ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_CORP_LEADERBOARD);
  }

}
