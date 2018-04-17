package enterprises.orbital.evekit.model.faction.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.FactionWarfareApi;
import enterprises.orbital.eve.esi.client.invoker.ApiException;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.GetFwWars200Ok;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.faction.FactionWar;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

public class ESIFacWarWarsSync extends AbstractESIRefSync<List<GetFwWars200Ok>> {
  protected static final Logger log = Logger.getLogger(ESIFacWarWarsSync.class.getName());

  @Override
  public ESIRefSyncEndpoint endpoint() {
    return ESIRefSyncEndpoint.REF_FW_WARS;
  }

  @Override
  protected void commit(long time,
                        RefCachedData item) throws IOException {
    assert item instanceof FactionWar;
    FactionWar api = (FactionWar) item;
    // Lookup only necessary if item is an update
    FactionWar existing = api.getLifeStart() == 0 ? FactionWar.get(time, api.getAgainstID(), api.getFactionID()) : null;
    evolveOrAdd(time, existing, api);
  }

  @Override
  protected ESIRefServerResult<List<GetFwWars200Ok>> getServerData(
      ESIRefClientProvider cp) throws ApiException, IOException {
    FactionWarfareApi apiInstance = cp.getFactionWarfareApi();
    ESIRefThrottle.throttle(endpoint().name());
    ApiResponse<List<GetFwWars200Ok>> result = apiInstance.getFwWarsWithHttpInfo(null, null, null);
    checkCommonProblems(result);
    return new ESIRefServerResult<>(extractExpiry(result, OrbitalProperties.getCurrentTime() + maxDelay()), result.getData());
  }

  @Override
  protected void processServerData(long time, ESIRefServerResult<List<GetFwWars200Ok>> data,
                                   List<RefCachedData> updates) throws IOException {
    List<GetFwWars200Ok> serverData = data.getData();
    // Create updates for all war entries
    Set<Pair<Integer, Integer>> seenWars = new HashSet<>();
    for (GetFwWars200Ok next : serverData) {
      updates.add(new FactionWar(next.getAgainstId(), next.getFactionId()));
      seenWars.add(Pair.of(next.getAgainstId(), next.getFactionId()));
    }
    // Look for any wars not contained in the update and schedule for EOL
    List<FactionWar> stored = retrieveAll(time, (long contid, AttributeSelector at) ->
        FactionWar.accessQuery(contid, 1000, false, at, ANY_SELECTOR, ANY_SELECTOR));
    for (FactionWar next : stored) {
      if (!seenWars.contains(Pair.of(next.getAgainstID(), next.getFactionID()))) {
        next.evolve(null, time);
        updates.add(next);
      }
    }
  }

}
