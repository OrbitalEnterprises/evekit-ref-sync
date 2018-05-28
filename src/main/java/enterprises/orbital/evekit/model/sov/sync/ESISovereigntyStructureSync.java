package enterprises.orbital.evekit.model.sov.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.SovereigntyApi;
import enterprises.orbital.eve.esi.client.invoker.ApiException;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.GetSovereigntyStructures200Ok;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.sov.SovereigntyStructure;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ESISovereigntyStructureSync extends AbstractESIRefSync<List<GetSovereigntyStructures200Ok>> {
  protected static final Logger log = Logger.getLogger(ESISovereigntyStructureSync.class.getName());

  @Override
  public ESIRefSyncEndpoint endpoint() {
    return ESIRefSyncEndpoint.REF_SOV_STRUCTURE;
  }

  @Override
  protected void commit(long time,
                        RefCachedData item) throws IOException {
    assert item instanceof SovereigntyStructure;
    SovereigntyStructure api = (SovereigntyStructure) item;
    // Lookup only necessary if item is an update
    SovereigntyStructure existing = api.getLifeStart() == 0 ? SovereigntyStructure.get(time,
                                                                                       api.getStructureID()) : null;
    evolveOrAdd(time, existing, api);
  }

  @Override
  protected ESIRefServerResult<List<GetSovereigntyStructures200Ok>> getServerData(
      ESIRefClientProvider cp) throws ApiException, IOException {
    SovereigntyApi apiInstance = cp.getSovereigntyApi();
    ESIRefThrottle.throttle(endpoint().name());
    ApiResponse<List<GetSovereigntyStructures200Ok>> result = apiInstance.getSovereigntyStructuresWithHttpInfo(null,
                                                                                                               null);
    checkCommonProblems(result);
    return new ESIRefServerResult<>(extractExpiry(result, OrbitalProperties.getCurrentTime() + maxDelay()),
                                    result.getData());
  }

  @Override
  protected void processServerData(long time, ESIRefServerResult<List<GetSovereigntyStructures200Ok>> data,
                                   List<RefCachedData> updates) throws IOException {
    List<GetSovereigntyStructures200Ok> serverData = data.getData();

    // Map all current existing structures
    List<SovereigntyStructure> stored = retrieveAll(time, (long contid, AttributeSelector at) ->
        SovereigntyStructure.accessQuery(contid, 1000, false, at, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                         ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR));
    Map<Long, SovereigntyStructure> current = new HashMap<>();
    for (SovereigntyStructure i : stored) {
      current.put(i.getStructureID(), i);
    }

    // Inspect structure updates.  If a structure is new or different from the current version, then schedule for update.
    for (GetSovereigntyStructures200Ok next : serverData) {
      SovereigntyStructure nextStructure = new SovereigntyStructure(next.getAllianceId(), next.getSolarSystemId(),
                                                                    next.getStructureId(), next.getStructureTypeId(),
                                                                    nullSafeFloat(next.getVulnerabilityOccupancyLevel(),
                                                                                  0F),
                                                                    nullSafeDateTime(next.getVulnerableStartTime(),
                                                                                     new DateTime(
                                                                                         new Date(0))).getMillis(),
                                                                    nullSafeDateTime(next.getVulnerableEndTime(),
                                                                                     new DateTime(
                                                                                         new Date(0))).getMillis());
      SovereigntyStructure existing = current.get(next.getStructureId());
      if (existing == null || !existing.equivalent(nextStructure)) {
        updates.add(nextStructure);
      }
      if (existing != null) {
        current.remove(next.getStructureId());
      }
    }

    // Anything left in the current map should be end of lifed
    for (SovereigntyStructure next : current.values()) {
      next.evolve(null, time);
      updates.add(next);
    }
  }

}
