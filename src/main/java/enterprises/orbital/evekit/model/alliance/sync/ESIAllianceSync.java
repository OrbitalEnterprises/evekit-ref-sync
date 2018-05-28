package enterprises.orbital.evekit.model.alliance.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.AllianceApi;
import enterprises.orbital.eve.esi.client.invoker.ApiException;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.GetAlliancesAllianceIdIconsOk;
import enterprises.orbital.eve.esi.client.model.GetAlliancesAllianceIdOk;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.alliance.Alliance;
import enterprises.orbital.evekit.model.alliance.AllianceIcon;
import enterprises.orbital.evekit.model.alliance.AllianceMemberCorporation;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ESIAllianceSync extends AbstractESIRefSync<ESIAllianceSync.AllianceServerData> {
  protected static final Logger log = Logger.getLogger(ESIAllianceSync.class.getName());
  private String context;

  class AllianceServerData {
    List<Integer> allianceList = new ArrayList<>();
    Map<Integer, GetAlliancesAllianceIdOk> allianceMap = new HashMap<>();
    Map<Integer, GetAlliancesAllianceIdIconsOk> iconMap = new HashMap<>();
    Map<Integer, List<Integer>> corpListMap = new HashMap<>();
  }

  @Override
  public ESIRefSyncEndpoint endpoint() {
    return ESIRefSyncEndpoint.REF_ALLIANCE;
  }

  @Override
  protected String getNextSyncContext() {
    return context;
  }

  @Override
  protected void commit(long time,
                        RefCachedData item) throws IOException {
    assert item instanceof Alliance || item instanceof AllianceIcon || item instanceof AllianceMemberCorporation;
    RefCachedData existing = null;
    if (item.getLifeStart() == 0) {
      // Only need to check for existing item if current item is an update
      if (item instanceof Alliance) {
        existing = Alliance.get(time, ((Alliance) item).getAllianceID());
      } else if (item instanceof AllianceIcon) {
        existing = AllianceIcon.get(time, ((AllianceIcon) item).getAllianceID());
      } else {
        AllianceMemberCorporation api = (AllianceMemberCorporation) item;
        existing = AllianceMemberCorporation.get(time, api.getAllianceID(), api.getCorporationID());
      }
    }
    evolveOrAdd(time, existing, item);
  }

  /**
   * Runner class for making threaded requests to the ESI.  The alliance endpoints require separate
   * individual calls for many data items.  Therefore, threading is required to avoid long synchronization times.
   *
   * @param <A> return type from the ESI call.
   */
  class ESICaller<A> implements Runnable {

    Map<Integer, Object> resultMap;
    int key;
    Callable<A> invoke;

    ESICaller(Map<Integer, Object> resultMap, int key, Callable<A> invoke) {
      this.resultMap = resultMap;
      this.key = key;
      this.invoke = invoke;
    }

    public void run() {
      try {
        resultMap.put(key, invoke.call());
      } catch (Throwable e) {
        resultMap.put(key, e);
      }
    }

  }

  @SuppressWarnings("unchecked")
  @Override
  protected ESIRefServerResult<AllianceServerData> getServerData(
      ESIRefClientProvider cp) throws ApiException, IOException {
    AllianceServerData resultData = new AllianceServerData();
    AllianceApi apiInstance = cp.getAllianceApi();
    // Retrieve alliance list
    log.fine(getContext() + " retrieving alliance list");
    ESIRefThrottle.throttle(endpoint().name());
    ApiResponse<List<Integer>> resultAllianceList = apiInstance.getAlliancesWithHttpInfo(null, null);
    checkCommonProblems(resultAllianceList);
    // Hard code expiry to six minutes in the future so that we properly cycle through all the alliances
    long expiry = OrbitalProperties.getCurrentTime() + TimeUnit.MILLISECONDS.convert(6, TimeUnit.MINUTES);

    // If a context is present, use it to filter out which alliances we'll process.
    // We batch alliance sync since the set of alliances is so large.
    int allianceFilter;
    try {
      allianceFilter = Integer.valueOf(getCurrentTracker().getContext());
      allianceFilter = Math.max(allianceFilter, 0);
    } catch (Exception e) {
      // No filter exists, assign a random filter
      allianceFilter = (int) ((OrbitalProperties.getCurrentTime() / 1000) % 10);
    }

    // Filter alliance list by alliance filter to produce target batch.
    final int allianceBatch = allianceFilter;
    resultData.allianceList.addAll(resultAllianceList.getData()
                                                     .stream()
                                                     .filter(x -> (x % 10) == allianceBatch)
                                                     .collect(Collectors.toList()));
    int allianceCount = resultData.allianceList.size();
    log.fine(getContext() + " done retrieving alliance list: " + resultData.allianceList.size() + " retrieved");

    // Prepare filter and context for next tracker
    allianceFilter = (allianceFilter + 1) % 10;
    context = String.valueOf(allianceFilter);

    // Retrieve alliance details
    log.fine(getContext() + " retrieving alliance details");
    final int BATCH_SIZE = 100;
    Map<Integer, Object> asyncCallMap = Collections.synchronizedMap(new HashMap<>());
    Map<Integer, Integer> allianceIDMap = new HashMap<>();
    int resultKey = 0;

    for (int resultCount = 0; resultCount < allianceCount; resultCount += BATCH_SIZE) {
      // Make the next (max) BATCH_SIZE asynchronous calls
      int expecting = Math.min(resultCount + BATCH_SIZE, allianceCount);
      log.fine(getContext() + " starting next batch up to " + expecting + " results");
      List<Integer> nextAllianceList = resultData.allianceList.subList(resultCount, expecting);

      for (int nextAlliance : nextAllianceList) {
        // Submit alliance data request
        allianceIDMap.put(resultKey, nextAlliance);
        cp.getScheduler()
          .submit(new ESICaller<>(asyncCallMap, resultKey++, () -> {
            try {
              ESIRefThrottle.throttle(endpoint().name());
              ApiResponse<GetAlliancesAllianceIdOk> resultAlliance = apiInstance.getAlliancesAllianceIdWithHttpInfo(
                  nextAlliance, null, null);
              checkCommonProblems(resultAlliance);
              return resultAlliance.getData();
            } catch (ApiException e) {
              ESIRefThrottle.throttle(e);
              throw e;
            }
          }));

        // Submit alliance icon request
        allianceIDMap.put(resultKey, nextAlliance);
        cp.getScheduler()
          .submit(new ESICaller<>(asyncCallMap, resultKey++, () -> {
            try {
              ESIRefThrottle.throttle(endpoint().name());
              ApiResponse<GetAlliancesAllianceIdIconsOk> resultIcons = apiInstance.getAlliancesAllianceIdIconsWithHttpInfo(
                  nextAlliance, null, null);
              checkCommonProblems(resultIcons);
              return resultIcons.getData();
            } catch (ApiException e) {
              ESIRefThrottle.throttle(e);
              throw e;
            }
          }));

        // Submit alliance corporations request
        allianceIDMap.put(resultKey, nextAlliance);
        cp.getScheduler()
          .submit(new ESICaller<>(asyncCallMap, resultKey++, () -> {
            try {
              ESIRefThrottle.throttle(endpoint().name());
              ApiResponse<List<Integer>> resultCorpList = apiInstance.getAlliancesAllianceIdCorporationsWithHttpInfo(
                  nextAlliance, null, null);
              checkCommonProblems(resultCorpList);
              return resultCorpList.getData();
            } catch (ApiException e) {
              ESIRefThrottle.throttle(e);
              throw e;
            }
          }));
      }
      synchronized (asyncCallMap) {
        while (asyncCallMap.size() < expecting * 3) {
          log.fine(getContext() + " waiting for " + String.valueOf(expecting * 3 - asyncCallMap.size()) + " responses");
          try {
            asyncCallMap.wait(TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS));
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
        }
      }
    }
    log.fine(getContext() + " processing asynchronous results");
    // Copy responses into data structure or throw exceptions as appropriate
    for (Map.Entry<Integer, Object> nextEntry : asyncCallMap.entrySet()) {
      int index = nextEntry.getKey();
      Object nextResult = nextEntry.getValue();
      if (nextResult instanceof ApiException)
        throw (ApiException) nextResult;
      if (nextResult instanceof IOException)
        throw (IOException) nextResult;
      int allianceID = allianceIDMap.get(index);
      if (nextResult instanceof GetAlliancesAllianceIdOk)
        resultData.allianceMap.put(allianceID, (GetAlliancesAllianceIdOk) nextResult);
      else if (nextResult instanceof GetAlliancesAllianceIdIconsOk)
        resultData.iconMap.put(allianceID, (GetAlliancesAllianceIdIconsOk) nextResult);
      else if (nextResult instanceof List)
        resultData.corpListMap.put(allianceID, (List<Integer>) nextResult);
      else
        throw new IOException("Result object has unexpected type: " + nextResult.getClass()
                                                                                .getName());
    }
    log.fine(getContext() + " done retrieving alliance details");
    return new ESIRefServerResult<>(expiry, resultData);
  }

  @SuppressWarnings("RedundantThrows")
  @Override
  protected void processServerData(long time, ESIRefServerResult<AllianceServerData> data,
                                   List<RefCachedData> updates) throws IOException {
    // Detect any missing alliances and mark their data for removal
    AllianceServerData serverData = data.getData();
    List<Alliance> existing = retrieveAll(time,
                                          (long contid, AttributeSelector at) -> Alliance.accessQuery(contid, 1000,
                                                                                                      false, at,
                                                                                                      ANY_SELECTOR,
                                                                                                      ANY_SELECTOR,
                                                                                                      ANY_SELECTOR,
                                                                                                      ANY_SELECTOR,
                                                                                                      ANY_SELECTOR,
                                                                                                      ANY_SELECTOR,
                                                                                                      ANY_SELECTOR,
                                                                                                      ANY_SELECTOR,
                                                                                                      ANY_SELECTOR));
    Set<Integer> current = new HashSet<>(serverData.allianceList);
    for (Alliance nextAlliance : existing) {
      if (!current.contains((int) nextAlliance.getAllianceID())) {
        // This alliance no longer exists.  EOL the Alliance, it's icon and any member corporations
        nextAlliance.evolve(null, time);
        updates.add(nextAlliance);
        // Handle icons associated with this alliance
        AllianceIcon existingIcon = AllianceIcon.get(time, nextAlliance.getAllianceID());
        if (existingIcon != null) {
          existingIcon.evolve(null, time);
          updates.add(existingIcon);
        }
        // Handle member corporations associated with this alliance
        AttributeSelector byAllianceID = new AttributeSelector("{ values: [" + nextAlliance.getAllianceID() + "]}");
        List<AllianceMemberCorporation> existingMembers = retrieveAll(time,
                                                                      (long contid, AttributeSelector at) -> AllianceMemberCorporation.accessQuery(
                                                                          contid, 1000, false, at, byAllianceID,
                                                                          ANY_SELECTOR));
        for (AllianceMemberCorporation nextCorp : existingMembers) {
          nextCorp.evolve(null, time);
          updates.add(nextCorp);
        }
      }
    }
    // Process new alliance list
    Map<Long, Alliance> alMap = new HashMap<>();
    for (Alliance na : existing) {
      alMap.put(na.getAllianceID(), na);
    }
    for (int allianceID : serverData.allianceList) {
      // Construct and add Alliance for update
      GetAlliancesAllianceIdOk allianceData = serverData.allianceMap.get(allianceID);
      assert allianceData != null;
      List<Integer> allianceCorpList = serverData.corpListMap.get(allianceID);
      assert allianceCorpList != null;
      // Skip update if unchanged
      Alliance na = new Alliance(allianceID,
                                 nullSafeInteger(allianceData.getExecutorCorporationId(), -1),
                                 allianceCorpList.size(),
                                 allianceData.getName(),
                                 allianceData.getTicker(),
                                 nullSafeDateTime(allianceData.getDateFounded(), new DateTime(new Date(0))).getMillis(),
                                 nullSafeInteger(allianceData.getCreatorId(), -1),
                                 nullSafeInteger(allianceData.getCreatorCorporationId(), -1),
                                 nullSafeInteger(allianceData.getFactionId(), 0));
      Alliance existingA = alMap.get((long) allianceID);
      if (existingA == null || !na.equivalent(existingA)) {
        updates.add(na);
      }
      // Construct and add AllianceIcon for update
      GetAlliancesAllianceIdIconsOk allianceIcon = serverData.iconMap.get(allianceID);
      assert allianceIcon != null;
      AllianceIcon existingIcon = AllianceIcon.get(time, allianceID);
      AllianceIcon ni = new AllianceIcon(allianceID, allianceIcon.getPx64x64(), allianceIcon.getPx128x128());
      if (existingIcon == null || !ni.equivalent(existingIcon)) {
        updates.add(ni);
      }
      // Construct and add AllianceMemberCorporations for update
      AttributeSelector byAllianceID = new AttributeSelector("{ values: [" + allianceID + "]}");
      List<AllianceMemberCorporation> existingMembers = retrieveAll(time,
                                                                    (long contid, AttributeSelector at) -> AllianceMemberCorporation.accessQuery(
                                                                        contid, 1000, false, at, byAllianceID,
                                                                        ANY_SELECTOR));
      Set<Long> storedMembers = new HashSet<>();
      for (AllianceMemberCorporation nc : existingMembers) {
        storedMembers.add(nc.getCorporationID());
      }
      for (int nextCorpID : allianceCorpList) {
        if (!storedMembers.contains((long) nextCorpID))
          updates.add(new AllianceMemberCorporation(allianceID, nextCorpID));
      }
      // Check for any corporations that are no longer members and delete
      Set<Integer> existingCorpMembers = new HashSet<>(allianceCorpList);
      for (AllianceMemberCorporation nextCorp : existingMembers) {
        if (!existingCorpMembers.contains((int) nextCorp.getCorporationID())) {
          nextCorp.evolve(null, time);
          updates.add(nextCorp);
        }
      }
    }
  }

}
