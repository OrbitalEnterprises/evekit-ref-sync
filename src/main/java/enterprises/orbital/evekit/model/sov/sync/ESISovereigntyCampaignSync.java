package enterprises.orbital.evekit.model.sov.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.SovereigntyApi;
import enterprises.orbital.eve.esi.client.invoker.ApiException;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.GetSovereigntyCampaigns200Ok;
import enterprises.orbital.eve.esi.client.model.GetSovereigntyCampaignsParticipant;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.sov.SovereigntyCampaign;
import enterprises.orbital.evekit.model.sov.SovereigntyCampaignParticipant;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class ESISovereigntyCampaignSync extends AbstractESIRefSync<List<GetSovereigntyCampaigns200Ok>> {
  protected static final Logger log = Logger.getLogger(ESISovereigntyCampaignSync.class.getName());

  @Override
  public ESIRefSyncEndpoint endpoint() {
    return ESIRefSyncEndpoint.REF_SOV_CAMPAIGN;
  }

  @Override
  protected void commit(long time,
                        RefCachedData item) throws IOException {
    assert item instanceof SovereigntyCampaign || item instanceof SovereigntyCampaignParticipant;
    RefCachedData existing = null;
    if (item.getLifeStart() == 0) {
      // Only need to check for existing item if current item is an update
      if (item instanceof SovereigntyCampaign) {
        SovereigntyCampaign api = (SovereigntyCampaign) item;
        existing = SovereigntyCampaign.get(time, api.getCampaignID());
      } else {
        SovereigntyCampaignParticipant api = (SovereigntyCampaignParticipant) item;
        existing = SovereigntyCampaignParticipant.get(time, api.getCampaignID(), api.getAllianceID());
      }
    }
    evolveOrAdd(time, existing, item);
  }

  @Override
  protected ESIRefServerResult<List<GetSovereigntyCampaigns200Ok>> getServerData(
      ESIRefClientProvider cp) throws ApiException, IOException {
    SovereigntyApi apiInstance = cp.getSovereigntyApi();
    ESIRefThrottle.throttle(endpoint().name());
    ApiResponse<List<GetSovereigntyCampaigns200Ok>> result = apiInstance.getSovereigntyCampaignsWithHttpInfo(null, null, null);
    checkCommonProblems(result);
    return new ESIRefServerResult<>(extractExpiry(result, OrbitalProperties.getCurrentTime() + maxDelay()), result.getData());
  }

  @Override
  protected void processServerData(long time, ESIRefServerResult<List<GetSovereigntyCampaigns200Ok>> data,
                                   List<RefCachedData> updates) throws IOException {
    List<GetSovereigntyCampaigns200Ok> serverData = data.getData();
    // Create updates for all campaigns and participants
    Set<Integer> seenCampaigns = new HashSet<>();
    Map<Integer, Set<Integer>> seenParts = new HashMap<>();
    for (GetSovereigntyCampaigns200Ok next : serverData) {
      updates.add(new SovereigntyCampaign(next.getCampaignId(), next.getStructureId(), next.getSolarSystemId(),
                                          next.getConstellationId(), next.getEventType()
                                                                         .toString(), next.getStartTime()
                                                                                          .getMillis(),
                                          nullSafeInteger(next.getDefenderId(), 0), nullSafeFloat(next.getDefenderScore(), 0F),
                                          nullSafeFloat(next.getAttackersScore(), 0F)));
      seenCampaigns.add(next.getCampaignId());
      List<GetSovereigntyCampaignsParticipant> parts = next.getParticipants();
      if (!parts.isEmpty()) {
        Set<Integer> seenAlliances = new HashSet<>();
        seenParts.put(next.getCampaignId(), seenAlliances);
        for (GetSovereigntyCampaignsParticipant nextPart : parts) {
          updates.add(new SovereigntyCampaignParticipant(next.getCampaignId(), nextPart.getAllianceId(), nextPart.getScore()));
          seenAlliances.add(nextPart.getAllianceId());
        }
      }
    }
    // Look for any campaigns or participants not contained in the update and schedule for EOL
    List<SovereigntyCampaign> stored = retrieveAll(time, (long contid, AttributeSelector at) ->
        SovereigntyCampaign.accessQuery(contid, 1000, false, at, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR));
    for (SovereigntyCampaign next : stored) {
      if (!seenCampaigns.contains(next.getCampaignID())) {
        next.evolve(null, time);
        updates.add(next);
      }
    }
    List<SovereigntyCampaignParticipant> storedParts = retrieveAll(time, (long contid, AttributeSelector at) ->
        SovereigntyCampaignParticipant.accessQuery(contid, 1000, false, at, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR));
    for (SovereigntyCampaignParticipant next : storedParts) {
      if (!seenParts.containsKey(next.getCampaignID()) || !seenParts.get(next.getCampaignID())
                                                                    .contains(next.getAllianceID())) {
        next.evolve(null, time);
        updates.add(next);
      }
    }
  }

}
