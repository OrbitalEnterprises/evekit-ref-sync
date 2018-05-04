package enterprises.orbital.evekit.model.faction.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.FactionWarfareApi;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.*;
import enterprises.orbital.evekit.TestBase;
import enterprises.orbital.evekit.account.EveKitRefDataProvider;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.faction.*;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("Duplicates")
public class ESIFacWarCorporationLeaderboardSyncTest extends RefTestBase {

  // Local mocks and other objects
  private ESIRefClientProvider mockServer;
  private FactionWarfareApi mockEndpoint;
  private long testTime = 1238L;

  private static StatAttribute[] attChoices = new StatAttribute[]{
      StatAttribute.LAST_WEEK, StatAttribute.TOTAL, StatAttribute.YESTERDAY
  };

  private static Object[][] corpLeaderboardKillsTestData;
  private static Object[][] corpLeaderboardVPsTestData;

  static {
    // Faction war leaderboard kills test data
    // 0 int corporationID;
    // 1 StatAttribute attribute
    // 2 int kills
    int size = 20 + TestBase.getRandomInt(20);
    corpLeaderboardKillsTestData = new Object[size][3];
    for (int i = 0; i < size; i++) {
      corpLeaderboardKillsTestData[i][0] = TestBase.getUniqueRandomInteger();
      corpLeaderboardKillsTestData[i][1] = attChoices[TestBase.getRandomInt(attChoices.length)];
      corpLeaderboardKillsTestData[i][2] = TestBase.getRandomInt();
    }

    // Faction war leaderboard victory points test data
    // 0 int corporationID;
    // 1 int attribute (indexes attChoices)
    // 2 int victory points
    size = 20 + TestBase.getRandomInt(20);
    corpLeaderboardVPsTestData = new Object[size][3];
    for (int i = 0; i < size; i++) {
      corpLeaderboardVPsTestData[i][0] = TestBase.getUniqueRandomInteger();
      corpLeaderboardVPsTestData[i][1] = attChoices[TestBase.getRandomInt(attChoices.length)];
      corpLeaderboardVPsTestData[i][2] = TestBase.getRandomInt();
    }
  }

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();

    // Prepare a test sync tracker
    ESIRefEndpointSyncTracker.getOrCreateUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_CORP_LEADERBOARD, 1234L, null);

    // Initialize time keeper
    OrbitalProperties.setTimeGenerator(() -> testTime);
  }

  @Override
  @After
  public void teardown() throws Exception {
    // Cleanup test specific tables after each test
    EveKitRefDataProvider.getFactory()
                         .runTransaction(() -> {
                           EveKitRefDataProvider.getFactory()
                                                .getEntityManager()
                                                .createQuery("DELETE FROM CorporationKillStat ")
                                                .executeUpdate();
                           EveKitRefDataProvider.getFactory()
                                                .getEntityManager()
                                                .createQuery("DELETE FROM CorporationVictoryPointStat ")
                                                .executeUpdate();
                         });
    OrbitalProperties.setTimeGenerator(null);
    super.teardown();
  }

  // Mock up server interface
  private void setupMock() throws Exception {
    // Setup endpoint mock
    mockEndpoint = EasyMock.createMock(FactionWarfareApi.class);

    // Setup faction leaderboard call
    GetFwLeaderboardsCorporationsOk corpLeaderboard = new GetFwLeaderboardsCorporationsOk();
    GetFwLeaderboardsCorporationsKills kills = new GetFwLeaderboardsCorporationsKills();
    GetFwLeaderboardsCorporationsVictoryPoints vps = new GetFwLeaderboardsCorporationsVictoryPoints();
    corpLeaderboard.setKills(kills);
    corpLeaderboard.setVictoryPoints(vps);
    // Set kills data
    kills.setActiveTotal(Arrays.stream(corpLeaderboardKillsTestData)
                               .filter(x -> x[1] == StatAttribute.TOTAL)
                               .map(x -> {
                                 GetFwLeaderboardsCorporationsActiveTotal stat = new GetFwLeaderboardsCorporationsActiveTotal();
                                 stat.setCorporationId((Integer) x[0]);
                                 stat.setAmount((Integer) x[2]);
                                 return stat;
                               })
                               .collect(Collectors.toList()));
    kills.setLastWeek(Arrays.stream(corpLeaderboardKillsTestData)
                            .filter(x -> x[1] == StatAttribute.LAST_WEEK)
                            .map(x -> {
                              GetFwLeaderboardsCorporationsLastWeek stat = new GetFwLeaderboardsCorporationsLastWeek();
                              stat.setCorporationId((Integer) x[0]);
                              stat.setAmount((Integer) x[2]);
                              return stat;
                            })
                            .collect(Collectors.toList()));
    kills.setYesterday(Arrays.stream(corpLeaderboardKillsTestData)
                             .filter(x -> x[1] == StatAttribute.YESTERDAY)
                             .map(x -> {
                               GetFwLeaderboardsCorporationsYesterday stat = new GetFwLeaderboardsCorporationsYesterday();
                               stat.setCorporationId((Integer) x[0]);
                               stat.setAmount((Integer) x[2]);
                               return stat;
                             })
                             .collect(Collectors.toList()));
    // Set victory points data
    vps.setActiveTotal(Arrays.stream(corpLeaderboardVPsTestData)
                             .filter(x -> x[1] == StatAttribute.TOTAL)
                             .map(x -> {
                               GetFwLeaderboardsCorporationsActiveTotal1 stat = new GetFwLeaderboardsCorporationsActiveTotal1();
                               stat.setCorporationId((Integer) x[0]);
                               stat.setAmount((Integer) x[2]);
                               return stat;
                             })
                             .collect(Collectors.toList()));
    vps.setLastWeek(Arrays.stream(corpLeaderboardVPsTestData)
                          .filter(x -> x[1] == StatAttribute.LAST_WEEK)
                          .map(x -> {
                            GetFwLeaderboardsCorporationsLastWeek1 stat = new GetFwLeaderboardsCorporationsLastWeek1();
                            stat.setCorporationId((Integer) x[0]);
                            stat.setAmount((Integer) x[2]);
                            return stat;
                          })
                          .collect(Collectors.toList()));
    vps.setYesterday(Arrays.stream(corpLeaderboardVPsTestData)
                           .filter(x -> x[1] == StatAttribute.YESTERDAY)
                           .map(x -> {
                             GetFwLeaderboardsCorporationsYesterday1 stat = new GetFwLeaderboardsCorporationsYesterday1();
                             stat.setCorporationId((Integer) x[0]);
                             stat.setAmount((Integer) x[2]);
                             return stat;
                           })
                           .collect(Collectors.toList()));

    // Setup call
    Map<String, List<String>> headers = createHeaders("Expires", "Thu, 21 Dec 2017 12:00:00 GMT");
    ApiResponse<GetFwLeaderboardsCorporationsOk> corpLeaderboardsResponse = new ApiResponse<>(200, headers, corpLeaderboard);
    EasyMock.expect(mockEndpoint.getFwLeaderboardsCorporationsWithHttpInfo(null, null, null, null))
            .andReturn(corpLeaderboardsResponse);

    // Finally, setup client provider mock
    mockServer = EasyMock.createMock(ESIRefClientProvider.class);
    EasyMock.expect(mockServer.getFactionWarfareApi())
            .andReturn(mockEndpoint)
            .anyTimes();
  }

  private interface GetCorporation<A> {
    int getCorporationID(A src);
  }

  private interface GetAttribute<A> {
    StatAttribute getAttribute(A src);
  }

  private interface GetStat<A> {
    int getStat(A src);
  }

  private <A> void verifyMatchingData(List<Object[]> raw, List<A> stored, GetCorporation<A> gf, GetAttribute<A> ga,
                                      GetStat<A> gs) {
    Assert.assertEquals(raw.size(), stored.size());
    for (int i = 0; i < raw.size(); i++) {
      Object[] nextRaw = raw.get(i);
      A nextStored = stored.get(i);
      Assert.assertEquals((int) (Integer) nextRaw[0], gf.getCorporationID(nextStored));
      Assert.assertEquals(nextRaw[1], ga.getAttribute(nextStored));
      Assert.assertEquals((int) (Integer) nextRaw[2], gs.getStat(nextStored));
    }
  }

  private void verifyDataUpdate() throws Exception {
    // Retrieve all stored faction stats and victory points data
    List<CorporationKillStat> storedKills = AbstractESIRefSync.retrieveAll(testTime, (long contid, AttributeSelector at) ->
        CorporationKillStat.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));
    List<CorporationVictoryPointStat> storedVPs = AbstractESIRefSync.retrieveAll(testTime, (long contid, AttributeSelector at) ->
        CorporationVictoryPointStat.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(corpLeaderboardKillsTestData.length, storedKills.size());
    Assert.assertEquals(corpLeaderboardVPsTestData.length, storedVPs.size());

    // Check data
    verifyMatchingData(
        Arrays.stream(corpLeaderboardKillsTestData)
              .filter(x -> x[1] == StatAttribute.TOTAL)
              .collect(Collectors.toList()),
        storedKills.stream()
                   .filter(x -> x.getAttribute() == StatAttribute.TOTAL)
                   .collect(Collectors.toList()),
        CorporationKillStat::getCorporationID,
        AbstractKillStat::getAttribute,
        AbstractKillStat::getKills);
    verifyMatchingData(
        Arrays.stream(corpLeaderboardKillsTestData)
              .filter(x -> x[1] == StatAttribute.YESTERDAY)
              .collect(Collectors.toList()),
        storedKills.stream()
                   .filter(x -> x.getAttribute() == StatAttribute.YESTERDAY)
                   .collect(Collectors.toList()),
        CorporationKillStat::getCorporationID,
        AbstractKillStat::getAttribute,
        AbstractKillStat::getKills);
    verifyMatchingData(
        Arrays.stream(corpLeaderboardKillsTestData)
              .filter(x -> x[1] == StatAttribute.LAST_WEEK)
              .collect(Collectors.toList()),
        storedKills.stream()
                   .filter(x -> x.getAttribute() == StatAttribute.LAST_WEEK)
                   .collect(Collectors.toList()),
        CorporationKillStat::getCorporationID,
        AbstractKillStat::getAttribute,
        AbstractKillStat::getKills);
    verifyMatchingData(
        Arrays.stream(corpLeaderboardVPsTestData)
              .filter(x -> x[1] == StatAttribute.TOTAL)
              .collect(Collectors.toList()),
        storedVPs.stream()
                 .filter(x -> x.getAttribute() == StatAttribute.TOTAL)
                 .collect(Collectors.toList()),
        CorporationVictoryPointStat::getCorporationID,
        AbstractVictoryPointStat::getAttribute,
        AbstractVictoryPointStat::getVictoryPoints);
    verifyMatchingData(
        Arrays.stream(corpLeaderboardVPsTestData)
              .filter(x -> x[1] == StatAttribute.YESTERDAY)
              .collect(Collectors.toList()),
        storedVPs.stream()
                 .filter(x -> x.getAttribute() == StatAttribute.YESTERDAY)
                 .collect(Collectors.toList()),
        CorporationVictoryPointStat::getCorporationID,
        AbstractVictoryPointStat::getAttribute,
        AbstractVictoryPointStat::getVictoryPoints);
    verifyMatchingData(
        Arrays.stream(corpLeaderboardVPsTestData)
              .filter(x -> x[1] == StatAttribute.LAST_WEEK)
              .collect(Collectors.toList()),
        storedVPs.stream()
                 .filter(x -> x.getAttribute() == StatAttribute.LAST_WEEK)
                 .collect(Collectors.toList()),
        CorporationVictoryPointStat::getCorporationID,
        AbstractVictoryPointStat::getAttribute,
        AbstractVictoryPointStat::getVictoryPoints);
  }

  @Test
  public void testSyncUpdate() throws Exception {
    setupMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Perform the sync
    ESIFacWarCorporationLeaderboardSync sync = new ESIFacWarCorporationLeaderboardSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify updated properly
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_FW_CORP_LEADERBOARD);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_CORP_LEADERBOARD);
    long schedTime = (new DateTime(2017, 12, 21, 12, 0, 0, DateTimeZone.UTC)).getMillis();
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Populate existing
    //
    // Half the stats entries we insert are purposely not present in the server data so we can test deletion.
    // Compute the modified faction IDs
    int[] modifiedKillsCorporationIDs = new int[corpLeaderboardKillsTestData.length];
    for (int i = 0; i < modifiedKillsCorporationIDs.length; i++) {
      if (i % 2 == 0)
        modifiedKillsCorporationIDs[i] = TestBase.getUniqueRandomInteger();
      else
        modifiedKillsCorporationIDs[i] = (int) (Integer) corpLeaderboardKillsTestData[i][0];
    }
    int[] modifiedVPsCorporationIDs = new int[corpLeaderboardVPsTestData.length];
    for (int i = 0; i < modifiedVPsCorporationIDs.length; i++) {
      if (i % 2 == 0)
        modifiedVPsCorporationIDs[i] = TestBase.getUniqueRandomInteger();
      else
        modifiedVPsCorporationIDs[i] = (int) (Integer) corpLeaderboardVPsTestData[i][0];
    }
    // Populate existing stats
    for (int i = 0; i < corpLeaderboardKillsTestData.length; i++) {
      CorporationKillStat newStat = new CorporationKillStat((StatAttribute) corpLeaderboardKillsTestData[i][1],
                                                            (Integer) corpLeaderboardKillsTestData[i][2] + 1,
                                                            modifiedKillsCorporationIDs[i]);
      newStat.setup(testTime - 1);
      RefCachedData.update(newStat);
    }
    for (int i = 0; i < corpLeaderboardVPsTestData.length; i++) {
      CorporationVictoryPointStat newStat = new CorporationVictoryPointStat((StatAttribute) corpLeaderboardVPsTestData[i][1],
                                                                            (Integer) corpLeaderboardVPsTestData[i][2] + 1,
                                                                            modifiedVPsCorporationIDs[i]);
      newStat.setup(testTime - 1);
      RefCachedData.update(newStat);
    }

    // Perform the sync
    ESIFacWarCorporationLeaderboardSync sync = new ESIFacWarCorporationLeaderboardSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify old objects were evolved properly
    List<CorporationKillStat> oldKills = AbstractESIRefSync.retrieveAll(testTime - 1, (long contid, AttributeSelector at) ->
        CorporationKillStat.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));
    List<CorporationVictoryPointStat> oldVPs = AbstractESIRefSync.retrieveAll(testTime - 1, (long contid, AttributeSelector at) ->
        CorporationVictoryPointStat.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(corpLeaderboardKillsTestData.length, oldKills.size());
    Assert.assertEquals(corpLeaderboardVPsTestData.length, oldVPs.size());

    // Check old wars data
    for (int i = 0; i < corpLeaderboardKillsTestData.length; i++) {
      CorporationKillStat nextStat = oldKills.get(i);
      Assert.assertEquals(testTime, nextStat.getLifeEnd());
      Assert.assertEquals(modifiedKillsCorporationIDs[i], nextStat.getCorporationID());
      Assert.assertEquals(corpLeaderboardKillsTestData[i][1], nextStat.getAttribute());
      Assert.assertEquals((Integer) corpLeaderboardKillsTestData[i][2] + 1, nextStat.getKills());
    }
    for (int i = 0; i < corpLeaderboardVPsTestData.length; i++) {
      CorporationVictoryPointStat nextStat = oldVPs.get(i);
      Assert.assertEquals(testTime, nextStat.getLifeEnd());
      Assert.assertEquals(modifiedVPsCorporationIDs[i], nextStat.getCorporationID());
      Assert.assertEquals(corpLeaderboardVPsTestData[i][1], nextStat.getAttribute());
      Assert.assertEquals((Integer) corpLeaderboardVPsTestData[i][2] + 1, nextStat.getVictoryPoints());
    }

    // Verify updates which will also verify that all old alliances were properly end of lifed
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_FW_CORP_LEADERBOARD);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_CORP_LEADERBOARD);
    long schedTime = (new DateTime(2017, 12, 21, 12, 0, 0, DateTimeZone.UTC)).getMillis();
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

}
