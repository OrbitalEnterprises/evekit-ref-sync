package enterprises.orbital.evekit.model.faction.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.FactionWarfareApi;
import enterprises.orbital.eve.esi.client.invoker.ApiException;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.GetFwSystems200Ok;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.faction.FactionWarSystem;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

public class ESIFacWarSystemsSync extends AbstractESIRefSync<List<GetFwSystems200Ok>> {
  protected static final Logger log = Logger.getLogger(ESIFacWarSystemsSync.class.getName());

  @Override
  public ESIRefSyncEndpoint endpoint() {
    return ESIRefSyncEndpoint.REF_FW_SYSTEMS;
  }

  @Override
  protected void commit(long time,
                        RefCachedData item) throws IOException {
    assert item instanceof FactionWarSystem;
    FactionWarSystem api = (FactionWarSystem) item;
    // Lookup only necessary if item is an update
    FactionWarSystem existing = api.getLifeStart() == 0 ? FactionWarSystem.get(time, api.getSolarSystemID()) : null;
    evolveOrAdd(time, existing, api);
  }

  @Override
  protected ESIRefServerResult<List<GetFwSystems200Ok>> getServerData(
      ESIRefClientProvider cp) throws ApiException, IOException {
    FactionWarfareApi apiInstance = cp.getFactionWarfareApi();
    ApiResponse<List<GetFwSystems200Ok>> result = apiInstance.getFwSystemsWithHttpInfo(null, null, null);
    checkCommonProblems(result);
    return new ESIRefServerResult<>(extractExpiry(result, OrbitalProperties.getCurrentTime() + maxDelay()), result.getData());
  }

  @Override
  protected void processServerData(long time, ESIRefServerResult<List<GetFwSystems200Ok>> data,
                                   List<RefCachedData> updates) throws IOException {
    List<GetFwSystems200Ok> serverData = data.getData();
    // Create updates for all entries
    Set<Integer> seenSystems = new HashSet<>();
    for (GetFwSystems200Ok next : serverData) {
      updates.add(new FactionWarSystem(next.getOccupierFactionId(), next.getOwnerFactionId(), next.getSolarSystemId(), next.getVictoryPoints(), next.getVictoryPointsThreshold(), next.getContested()));
      seenSystems.add(next.getSolarSystemId());
    }
    // Look for any systems not contained in the update and schedule for EOL
    List<FactionWarSystem> stored = retrieveAll(time, (long contid, AttributeSelector at) ->
        FactionWarSystem.accessQuery(contid, 1000, false, at, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR));
    for (FactionWarSystem next : stored) {
      if (!seenSystems.contains(next.getSolarSystemID())) {
        next.evolve(null, time);
        updates.add(next);
      }
    }
  }

  @Override
  public ESIRefEndpointSyncTracker getCurrentTracker() throws IOException, TrackerNotFoundException {
    return ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_SYSTEMS);
  }

}
