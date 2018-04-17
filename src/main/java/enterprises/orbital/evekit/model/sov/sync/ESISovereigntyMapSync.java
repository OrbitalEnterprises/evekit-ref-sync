package enterprises.orbital.evekit.model.sov.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.SovereigntyApi;
import enterprises.orbital.eve.esi.client.invoker.ApiException;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.GetSovereigntyMap200Ok;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.sov.SovereigntyMap;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

public class ESISovereigntyMapSync extends AbstractESIRefSync<List<GetSovereigntyMap200Ok>> {
  protected static final Logger log = Logger.getLogger(ESISovereigntyMapSync.class.getName());

  @Override
  public ESIRefSyncEndpoint endpoint() {
    return ESIRefSyncEndpoint.REF_SOV_MAP;
  }

  @Override
  protected void commit(long time,
                        RefCachedData item) throws IOException {
    assert item instanceof SovereigntyMap;
    SovereigntyMap api = (SovereigntyMap) item;
    // Lookup only necessary if item is an update
    SovereigntyMap existing = api.getLifeStart() == 0 ? SovereigntyMap.get(time,api.getSystemID()) : null;
    evolveOrAdd(time, existing, api);
  }

  @Override
  protected ESIRefServerResult<List<GetSovereigntyMap200Ok>> getServerData(
      ESIRefClientProvider cp) throws ApiException, IOException {
    SovereigntyApi apiInstance = cp.getSovereigntyApi();
    ESIRefThrottle.throttle(endpoint().name());
    ApiResponse<List<GetSovereigntyMap200Ok>> result = apiInstance.getSovereigntyMapWithHttpInfo(null, null, null);
    checkCommonProblems(result);
    return new ESIRefServerResult<>(extractExpiry(result, OrbitalProperties.getCurrentTime() + maxDelay()), result.getData());
  }

  @Override
  protected void processServerData(long time, ESIRefServerResult<List<GetSovereigntyMap200Ok>> data,
                                   List<RefCachedData> updates) throws IOException {
    List<GetSovereigntyMap200Ok> serverData = data.getData();
    // Create updates for all map entries
    Set<Integer> seenSystems = new HashSet<>();
    for (GetSovereigntyMap200Ok next : serverData) {
      updates.add(new SovereigntyMap(nullSafeInteger(next.getAllianceId(), 0),
                                     nullSafeInteger(next.getCorporationId(), 0),
                                     nullSafeInteger(next.getFactionId(), 0),
                                     next.getSystemId()));
      seenSystems.add(next.getSystemId());
    }
    // Look for any systems not contained in the update and schedule for EOL
    List<SovereigntyMap> stored = retrieveAll(time, (long contid, AttributeSelector at) ->
        SovereigntyMap.accessQuery(contid, 1000, false, at, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR));
    for (SovereigntyMap next : stored) {
      if (!seenSystems.contains(next.getSystemID())) {
        next.evolve(null, time);
        updates.add(next);
      }
    }
  }

}
