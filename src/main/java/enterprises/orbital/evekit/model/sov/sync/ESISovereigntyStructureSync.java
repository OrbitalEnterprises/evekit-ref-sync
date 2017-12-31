package enterprises.orbital.evekit.model.sov.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.SovereigntyApi;
import enterprises.orbital.eve.esi.client.invoker.ApiException;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.GetSovereigntyStructures200Ok;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.server.ServerStatus;
import enterprises.orbital.evekit.model.sov.SovereigntyStructure;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
    SovereigntyStructure existing = api.getLifeStart() == 0 ? SovereigntyStructure.get(time,api.getStructureID()) : null;
    evolveOrAdd(time, existing, api);
  }

  @Override
  protected ESIRefServerResult<List<GetSovereigntyStructures200Ok>> getServerData(
      ESIClientProvider cp) throws ApiException, IOException {
    SovereigntyApi apiInstance = cp.getSovereigntyApi();
    ApiResponse<List<GetSovereigntyStructures200Ok>> result = apiInstance.getSovereigntyStructuresWithHttpInfo(null, null, null);
    checkCommonProblems(result);
    return new ESIRefServerResult<>(extractExpiry(result, OrbitalProperties.getCurrentTime() + maxDelay()), result.getData());
  }

  @Override
  protected void processServerData(long time, ESIRefServerResult<List<GetSovereigntyStructures200Ok>> data,
                                   List<RefCachedData> updates) throws IOException {
    List<GetSovereigntyStructures200Ok> serverData = data.getData();
    // Create updates for all structures
    Set<Long> seenStructures = new HashSet<>();
    for (GetSovereigntyStructures200Ok next : serverData) {
      updates.add(new SovereigntyStructure(next.getAllianceId(), next.getSolarSystemId(), next.getStructureId(), next.getStructureTypeId(),
                                           nullSafeFloat(next.getVulnerabilityOccupancyLevel(), 0F),
                                           nullSafeDateTime(next.getVulnerableStartTime(), new DateTime(new Date(0))).getMillis(),
                                           nullSafeDateTime(next.getVulnerableEndTime(), new DateTime(new Date(0))).getMillis()));
      seenStructures.add(next.getStructureId());
    }
    // Look for any structures not contained in the update and schedule for EOL
    List<SovereigntyStructure> stored = retrieveAll(time, (long contid, AttributeSelector at) ->
        SovereigntyStructure.accessQuery(contid, 1000, false, at, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR));
    for (SovereigntyStructure next : stored) {
      if (!seenStructures.contains(next.getStructureID())) {
        next.evolve(null, time);
        updates.add(next);
      }
    }
  }

  @Override
  public ESIRefEndpointSyncTracker getCurrentTracker() throws IOException, TrackerNotFoundException {
    return ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_SOV_STRUCTURE);
  }

}
