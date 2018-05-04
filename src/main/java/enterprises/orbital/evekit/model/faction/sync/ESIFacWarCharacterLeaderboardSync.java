package enterprises.orbital.evekit.model.faction.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.FactionWarfareApi;
import enterprises.orbital.eve.esi.client.invoker.ApiException;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.*;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.faction.CharacterKillStat;
import enterprises.orbital.evekit.model.faction.CharacterVictoryPointStat;
import enterprises.orbital.evekit.model.faction.StatAttribute;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ESIFacWarCharacterLeaderboardSync extends AbstractESIRefSync<GetFwLeaderboardsCharactersOk> {
  protected static final Logger log = Logger.getLogger(ESIFacWarCharacterLeaderboardSync.class.getName());

  @Override
  public ESIRefSyncEndpoint endpoint() {
    return ESIRefSyncEndpoint.REF_FW_CHAR_LEADERBOARD;
  }

  @Override
  protected void commit(long time,
                        RefCachedData item) throws IOException {
    assert item instanceof CharacterKillStat || item instanceof CharacterVictoryPointStat;
    RefCachedData existing;
    if (item instanceof CharacterKillStat) {
      CharacterKillStat api = (CharacterKillStat) item;
      existing = api.getLifeStart() == 0 ? CharacterKillStat.get(time, api.getAttribute(), api.getCharacterID()) : null;
    } else {
      CharacterVictoryPointStat api = (CharacterVictoryPointStat) item;
      existing = api.getLifeStart() == 0 ? CharacterVictoryPointStat.get(time, api.getAttribute(), api.getCharacterID()) : null;
    }
    evolveOrAdd(time, existing, item);
  }

  @Override
  protected ESIRefServerResult<GetFwLeaderboardsCharactersOk> getServerData(
      ESIRefClientProvider cp) throws ApiException, IOException {
    FactionWarfareApi apiInstance = cp.getFactionWarfareApi();
    ESIRefThrottle.throttle(endpoint().name());
    ApiResponse<GetFwLeaderboardsCharactersOk> result = apiInstance.getFwLeaderboardsCharactersWithHttpInfo(null, null, null, null);
    checkCommonProblems(result);
    return new ESIRefServerResult<>(extractExpiry(result, OrbitalProperties.getCurrentTime() + maxDelay()), result.getData());
  }

  private interface CharacterGetter<A> {
    Integer getCharacter(A instance);
  }

  private interface AmountGetter<A> {
    Integer getAmount(A instance);
  }

  private <A> List<CharacterKillStat> produceKillStatList(List<A> source, StatAttribute tp, CharacterGetter<A> fg,
                                                          AmountGetter<A> ag) {
    return source.stream()
                 .map((val) -> new CharacterKillStat(tp, ag.getAmount(val), fg.getCharacter(val)))
                 .collect(Collectors.toList());
  }

  private <A> List<CharacterVictoryPointStat> produceVPStatList(List<A> source, StatAttribute tp,
                                                                CharacterGetter<A> fg,
                                                                AmountGetter<A> ag) {
    return source.stream()
                 .map((val) -> new CharacterVictoryPointStat(tp, ag.getAmount(val), fg.getCharacter(val)))
                 .collect(Collectors.toList());
  }

  @Override
  protected void processServerData(long time, ESIRefServerResult<GetFwLeaderboardsCharactersOk> data,
                                   List<RefCachedData> updates) throws IOException {
    GetFwLeaderboardsCharactersOk serverData = data.getData();
    // Create updates for all entries.
    // Collect kills first
    Set<Pair<StatAttribute, Integer>> seenCharacterKills = new HashSet<>();
    List<CharacterKillStat> allKills = new ArrayList<>();
    allKills.addAll(produceKillStatList(serverData.getKills()
                                                  .getActiveTotal(), StatAttribute.TOTAL,
                                        GetFwLeaderboardsCharactersActiveTotal::getCharacterId,
                                        GetFwLeaderboardsCharactersActiveTotal::getAmount));
    allKills.addAll(produceKillStatList(serverData.getKills()
                                                  .getLastWeek(), StatAttribute.LAST_WEEK,
                                        GetFwLeaderboardsCharactersLastWeek::getCharacterId,
                                        GetFwLeaderboardsCharactersLastWeek::getAmount));
    allKills.addAll(produceKillStatList(serverData.getKills()
                                                  .getYesterday(), StatAttribute.YESTERDAY,
                                        GetFwLeaderboardsCharactersYesterday::getCharacterId,
                                        GetFwLeaderboardsCharactersYesterday::getAmount));
    for (CharacterKillStat next : allKills) {
      seenCharacterKills.add(Pair.of(next.getAttribute(), next.getCharacterID()));
    }
    updates.addAll(allKills);
    // Now collect victory points
    Set<Pair<StatAttribute, Integer>> seenCharacterVPs = new HashSet<>();
    List<CharacterVictoryPointStat> allVPs = new ArrayList<>();
    allVPs.addAll(produceVPStatList(serverData.getVictoryPoints()
                                              .getActiveTotal(), StatAttribute.TOTAL,
                                    GetFwLeaderboardsCharactersActiveTotal1::getCharacterId,
                                    GetFwLeaderboardsCharactersActiveTotal1::getAmount));
    allVPs.addAll(produceVPStatList(serverData.getVictoryPoints()
                                              .getLastWeek(), StatAttribute.LAST_WEEK,
                                    GetFwLeaderboardsCharactersLastWeek1::getCharacterId,
                                    GetFwLeaderboardsCharactersLastWeek1::getAmount));
    allVPs.addAll(produceVPStatList(serverData.getVictoryPoints()
                                              .getYesterday(), StatAttribute.YESTERDAY,
                                    GetFwLeaderboardsCharactersYesterday1::getCharacterId,
                                    GetFwLeaderboardsCharactersYesterday1::getAmount));
    for (CharacterVictoryPointStat next : allVPs) {
      seenCharacterVPs.add(Pair.of(next.getAttribute(), next.getCharacterID()));
    }
    updates.addAll(allVPs);
    // Look for any character/attribute pairs not contained in the update and schedule for EOL
    List<CharacterKillStat> storedKills = retrieveAll(time, (long contid, AttributeSelector at) ->
        CharacterKillStat.accessQuery(contid, 1000, false, at, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR));
    for (CharacterKillStat next : storedKills) {
      if (!seenCharacterKills.contains(Pair.of(next.getAttribute(), next.getCharacterID()))) {
        next.evolve(null, time);
        updates.add(next);
      }
    }
    List<CharacterVictoryPointStat> storedVPs = retrieveAll(time, (long contid, AttributeSelector at) ->
        CharacterVictoryPointStat.accessQuery(contid, 1000, false, at, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR));
    for (CharacterVictoryPointStat next : storedVPs) {
      if (!seenCharacterVPs.contains(Pair.of(next.getAttribute(), next.getCharacterID()))) {
        next.evolve(null, time);
        updates.add(next);
      }
    }
  }

}
