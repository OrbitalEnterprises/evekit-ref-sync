package enterprises.orbital.evekit.model.faction.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.FactionWarfareApi;
import enterprises.orbital.eve.esi.client.invoker.ApiException;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.GetFwStats200Ok;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.faction.FactionStats;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

public class ESIFacWarStatsSync extends AbstractESIRefSync<List<GetFwStats200Ok>> {
  protected static final Logger log = Logger.getLogger(ESIFacWarStatsSync.class.getName());

  @Override
  public ESIRefSyncEndpoint endpoint() {
    return ESIRefSyncEndpoint.REF_FW_STATS;
  }

  @Override
  protected void commit(long time,
                        RefCachedData item) throws IOException {
    assert item instanceof FactionStats;
    FactionStats api = (FactionStats) item;
    // Lookup only necessary if item is an update
    FactionStats existing = api.getLifeStart() == 0 ? FactionStats.get(time, api.getFactionID()) : null;
    evolveOrAdd(time, existing, api);
  }

  @Override
  protected ESIRefServerResult<List<GetFwStats200Ok>> getServerData(
      ESIRefClientProvider cp) throws ApiException, IOException {
    FactionWarfareApi apiInstance = cp.getFactionWarfareApi();
    ApiResponse<List<GetFwStats200Ok>> result = apiInstance.getFwStatsWithHttpInfo(null, null, null);
    checkCommonProblems(result);
    return new ESIRefServerResult<>(extractExpiry(result, OrbitalProperties.getCurrentTime() + maxDelay()), result.getData());
  }

  @Override
  protected void processServerData(long time, ESIRefServerResult<List<GetFwStats200Ok>> data,
                                   List<RefCachedData> updates) throws IOException {
    List<GetFwStats200Ok> serverData = data.getData();
    // Create updates for all entries
    Set<Integer> seenFactions = new HashSet<>();
    for (GetFwStats200Ok next : serverData) {
      updates.add(new FactionStats(next.getFactionId(), next.getKills().getLastWeek(),
                                   next.getKills().getTotal(), next.getKills().getYesterday(),
                                   next.getPilots(),
                                   next.getSystemsControlled(),
                                   next.getVictoryPoints().getLastWeek(),
                                   next.getVictoryPoints().getTotal(),
                                   next.getVictoryPoints().getYesterday()));
      seenFactions.add(next.getFactionId());
    }
    // Look for any factions not contained in the update and schedule for EOL
    List<FactionStats> stored = retrieveAll(time, (long contid, AttributeSelector at) ->
        FactionStats.accessQuery(contid, 1000, false, at, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR));
    for (FactionStats next : stored) {
      if (!seenFactions.contains(next.getFactionID())) {
        next.evolve(null, time);
        updates.add(next);
      }
    }
  }

  @Override
  public ESIRefEndpointSyncTracker getCurrentTracker() throws IOException, TrackerNotFoundException {
    return ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_STATS);
  }

}
