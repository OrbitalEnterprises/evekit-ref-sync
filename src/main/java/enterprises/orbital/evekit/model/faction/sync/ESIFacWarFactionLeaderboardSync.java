package enterprises.orbital.evekit.model.faction.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.FactionWarfareApi;
import enterprises.orbital.eve.esi.client.invoker.ApiException;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.*;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.faction.FactionKillStat;
import enterprises.orbital.evekit.model.faction.FactionVictoryPointStat;
import enterprises.orbital.evekit.model.faction.StatAttribute;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ESIFacWarFactionLeaderboardSync extends AbstractESIRefSync<GetFwLeaderboardsOk> {
  protected static final Logger log = Logger.getLogger(ESIFacWarFactionLeaderboardSync.class.getName());

  @Override
  public ESIRefSyncEndpoint endpoint() {
    return ESIRefSyncEndpoint.REF_FW_FACTION_LEADERBOARD;
  }

  @Override
  protected void commit(long time,
                        RefCachedData item) throws IOException {
    assert item instanceof FactionKillStat || item instanceof FactionVictoryPointStat;
    RefCachedData existing;
    if (item instanceof FactionKillStat) {
      FactionKillStat api = (FactionKillStat) item;
      existing = api.getLifeStart() == 0 ? FactionKillStat.get(time, api.getAttribute(), api.getFactionID()) : null;
    } else {
      FactionVictoryPointStat api = (FactionVictoryPointStat) item;
      existing = api.getLifeStart() == 0 ? FactionVictoryPointStat.get(time, api.getAttribute(), api.getFactionID()) : null;
    }
    evolveOrAdd(time, existing, item);
  }

  @Override
  protected ESIRefServerResult<GetFwLeaderboardsOk> getServerData(
      ESIRefClientProvider cp) throws ApiException, IOException {
    FactionWarfareApi apiInstance = cp.getFactionWarfareApi();
    ESIRefThrottle.throttle(endpoint().name());
    ApiResponse<GetFwLeaderboardsOk> result = apiInstance.getFwLeaderboardsWithHttpInfo(null, null);
    checkCommonProblems(result);
    return new ESIRefServerResult<>(extractExpiry(result, OrbitalProperties.getCurrentTime() + maxDelay()), result.getData());
  }

  private interface FactionGetter<A> {
    Integer getFaction(A instance);
  }

  private interface AmountGetter<A> {
    Integer getAmount(A instance);
  }

  private <A> List<FactionKillStat> produceKillStatList(List<A> source, StatAttribute tp, FactionGetter<A> fg,
                                                        AmountGetter<A> ag) {
    return source.stream()
                 .map((val) -> new FactionKillStat(tp, ag.getAmount(val), fg.getFaction(val)))
                 .collect(Collectors.toList());
  }

  private <A> List<FactionVictoryPointStat> produceVPStatList(List<A> source, StatAttribute tp, FactionGetter<A> fg,
                                                              AmountGetter<A> ag) {
    return source.stream()
                 .map((val) -> new FactionVictoryPointStat(tp, ag.getAmount(val), fg.getFaction(val)))
                 .collect(Collectors.toList());
  }

  @Override
  protected void processServerData(long time, ESIRefServerResult<GetFwLeaderboardsOk> data,
                                   List<RefCachedData> updates) throws IOException {
    GetFwLeaderboardsOk serverData = data.getData();
    // Create updates for all entries.
    // Collect kills first
    Set<Pair<StatAttribute, Integer>> seenFactionKills = new HashSet<>();
    List<FactionKillStat> allKills = new ArrayList<>();
    allKills.addAll(produceKillStatList(serverData.getKills()
                                                  .getActiveTotal(), StatAttribute.TOTAL,
                                        GetFwLeaderboardsActiveTotalActiveTotal::getFactionId,
                                        GetFwLeaderboardsActiveTotalActiveTotal::getAmount));
    allKills.addAll(produceKillStatList(serverData.getKills()
                                                  .getLastWeek(), StatAttribute.LAST_WEEK,
                                        GetFwLeaderboardsLastWeekLastWeek::getFactionId,
                                        GetFwLeaderboardsLastWeekLastWeek::getAmount));
    allKills.addAll(produceKillStatList(serverData.getKills()
                                                  .getYesterday(), StatAttribute.YESTERDAY,
                                        GetFwLeaderboardsYesterdayYesterday::getFactionId,
                                        GetFwLeaderboardsYesterdayYesterday::getAmount));
    for (FactionKillStat next : allKills) {
      seenFactionKills.add(Pair.of(next.getAttribute(), next.getFactionID()));
    }
    updates.addAll(allKills);
    // Now collect victory points
    Set<Pair<StatAttribute, Integer>> seenFactionVPs = new HashSet<>();
    List<FactionVictoryPointStat> allVPs = new ArrayList<>();
    allVPs.addAll(produceVPStatList(serverData.getVictoryPoints()
                                              .getActiveTotal(), StatAttribute.TOTAL,
                                    GetFwLeaderboardsActiveTotalActiveTotal1::getFactionId,
                                    GetFwLeaderboardsActiveTotalActiveTotal1::getAmount));
    allVPs.addAll(produceVPStatList(serverData.getVictoryPoints()
                                              .getLastWeek(), StatAttribute.LAST_WEEK,
                                    GetFwLeaderboardsLastWeekLastWeek1::getFactionId,
                                    GetFwLeaderboardsLastWeekLastWeek1::getAmount));
    allVPs.addAll(produceVPStatList(serverData.getVictoryPoints()
                                              .getYesterday(), StatAttribute.YESTERDAY,
                                    GetFwLeaderboardsYesterdayYesterday1::getFactionId,
                                    GetFwLeaderboardsYesterdayYesterday1::getAmount));
    for (FactionVictoryPointStat next : allVPs) {
      seenFactionVPs.add(Pair.of(next.getAttribute(), next.getFactionID()));
    }
    updates.addAll(allVPs);
    // Look for any faction/attribute pairs not contained in the update and schedule for EOL
    List<FactionKillStat> storedKills = retrieveAll(time, (long contid, AttributeSelector at) ->
        FactionKillStat.accessQuery(contid, 1000, false, at, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR));
    for (FactionKillStat next : storedKills) {
      if (!seenFactionKills.contains(Pair.of(next.getAttribute(), next.getFactionID()))) {
        next.evolve(null, time);
        updates.add(next);
      }
    }
    List<FactionVictoryPointStat> storedVPs = retrieveAll(time, (long contid, AttributeSelector at) ->
        FactionVictoryPointStat.accessQuery(contid, 1000, false, at, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR));
    for (FactionVictoryPointStat next : storedVPs) {
      if (!seenFactionVPs.contains(Pair.of(next.getAttribute(), next.getFactionID()))) {
        next.evolve(null, time);
        updates.add(next);
      }
    }
  }

}
