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
public class ESIFacWarFactionLeaderboardSyncTest extends RefTestBase {

  // Local mocks and other objects
  private ESIRefClientProvider mockServer;
  private FactionWarfareApi mockEndpoint;
  private long testTime = 1238L;

  private static StatAttribute[] attChoices = new StatAttribute[]{
      StatAttribute.LAST_WEEK, StatAttribute.TOTAL, StatAttribute.YESTERDAY
  };

  private static Object[][] factionLeaderboardKillsTestData;
  private static Object[][] factionLeaderboardVPsTestData;

  static {
    // Faction war leaderboard kills test data
    // 0 int factionID;
    // 1 StatAttribute attribute
    // 2 int kills
    int size = 20 + TestBase.getRandomInt(20);
    factionLeaderboardKillsTestData = new Object[size][3];
    for (int i = 0; i < size; i++) {
      factionLeaderboardKillsTestData[i][0] = TestBase.getUniqueRandomInteger();
      factionLeaderboardKillsTestData[i][1] = attChoices[TestBase.getRandomInt(attChoices.length)];
      factionLeaderboardKillsTestData[i][2] = TestBase.getRandomInt();
    }

    // Faction war leaderboard victory points test data
    // 0 int factionID;
    // 1 int attribute (indexes attChoices)
    // 2 int victory points
    size = 20 + TestBase.getRandomInt(20);
    factionLeaderboardVPsTestData = new Object[size][3];
    for (int i = 0; i < size; i++) {
      factionLeaderboardVPsTestData[i][0] = TestBase.getUniqueRandomInteger();
      factionLeaderboardVPsTestData[i][1] = attChoices[TestBase.getRandomInt(attChoices.length)];
      factionLeaderboardVPsTestData[i][2] = TestBase.getRandomInt();
    }
  }

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();

    // Prepare a test sync tracker
    ESIRefEndpointSyncTracker.getOrCreateUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_FACTION_LEADERBOARD, 1234L, null);

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
                                                .createQuery("DELETE FROM FactionKillStat ")
                                                .executeUpdate();
                           EveKitRefDataProvider.getFactory()
                                                .getEntityManager()
                                                .createQuery("DELETE FROM FactionVictoryPointStat ")
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
    GetFwLeaderboardsOk factionLeaderboard = new GetFwLeaderboardsOk();
    GetFwLeaderboardsKills kills = new GetFwLeaderboardsKills();
    GetFwLeaderboardsVictoryPoints vps = new GetFwLeaderboardsVictoryPoints();
    factionLeaderboard.setKills(kills);
    factionLeaderboard.setVictoryPoints(vps);
    // Set kills data
    kills.setActiveTotal(Arrays.stream(factionLeaderboardKillsTestData)
                               .filter(x -> x[1] == StatAttribute.TOTAL)
                               .map(x -> {
                                 GetFwLeaderboardsActiveTotalActiveTotal stat = new GetFwLeaderboardsActiveTotalActiveTotal();
                                 stat.setFactionId((Integer) x[0]);
                                 stat.setAmount((Integer) x[2]);
                                 return stat;
                               })
                               .collect(Collectors.toList()));
    kills.setLastWeek(Arrays.stream(factionLeaderboardKillsTestData)
                            .filter(x -> x[1] == StatAttribute.LAST_WEEK)
                            .map(x -> {
                              GetFwLeaderboardsLastWeekLastWeek stat = new GetFwLeaderboardsLastWeekLastWeek();
                              stat.setFactionId((Integer) x[0]);
                              stat.setAmount((Integer) x[2]);
                              return stat;
                            })
                            .collect(Collectors.toList()));
    kills.setYesterday(Arrays.stream(factionLeaderboardKillsTestData)
                             .filter(x -> x[1] == StatAttribute.YESTERDAY)
                             .map(x -> {
                               GetFwLeaderboardsYesterdayYesterday stat = new GetFwLeaderboardsYesterdayYesterday();
                               stat.setFactionId((Integer) x[0]);
                               stat.setAmount((Integer) x[2]);
                               return stat;
                             })
                             .collect(Collectors.toList()));
    // Set victory points data
    vps.setActiveTotal(Arrays.stream(factionLeaderboardVPsTestData)
                             .filter(x -> x[1] == StatAttribute.TOTAL)
                             .map(x -> {
                               GetFwLeaderboardsActiveTotalActiveTotal1 stat = new GetFwLeaderboardsActiveTotalActiveTotal1();
                               stat.setFactionId((Integer) x[0]);
                               stat.setAmount((Integer) x[2]);
                               return stat;
                             })
                             .collect(Collectors.toList()));
    vps.setLastWeek(Arrays.stream(factionLeaderboardVPsTestData)
                          .filter(x -> x[1] == StatAttribute.LAST_WEEK)
                          .map(x -> {
                            GetFwLeaderboardsLastWeekLastWeek1 stat = new GetFwLeaderboardsLastWeekLastWeek1();
                            stat.setFactionId((Integer) x[0]);
                            stat.setAmount((Integer) x[2]);
                            return stat;
                          })
                          .collect(Collectors.toList()));
    vps.setYesterday(Arrays.stream(factionLeaderboardVPsTestData)
                           .filter(x -> x[1] == StatAttribute.YESTERDAY)
                           .map(x -> {
                             GetFwLeaderboardsYesterdayYesterday1 stat = new GetFwLeaderboardsYesterdayYesterday1();
                             stat.setFactionId((Integer) x[0]);
                             stat.setAmount((Integer) x[2]);
                             return stat;
                           })
                           .collect(Collectors.toList()));

    // Setup call
    Map<String, List<String>> headers = createHeaders("Expires", "Thu, 21 Dec 2017 12:00:00 GMT");
    ApiResponse<GetFwLeaderboardsOk> factionLeaderboardsResponse = new ApiResponse<>(200, headers, factionLeaderboard);
    EasyMock.expect(mockEndpoint.getFwLeaderboardsWithHttpInfo(null, null, null, null))
            .andReturn(factionLeaderboardsResponse);

    // Finally, setup client provider mock
    mockServer = EasyMock.createMock(ESIRefClientProvider.class);
    EasyMock.expect(mockServer.getFactionWarfareApi())
            .andReturn(mockEndpoint)
            .anyTimes();
  }

  private interface GetFaction<A> {
    int getFactionID(A src);
  }

  private interface GetAttribute<A> {
    StatAttribute getAttribute(A src);
  }

  private interface GetStat<A> {
    int getStat(A src);
  }

  private <A> void verifyMatchingData(List<Object[]> raw, List<A> stored, GetFaction<A> gf, GetAttribute<A> ga,
                                      GetStat<A> gs) {
    Assert.assertEquals(raw.size(), stored.size());
    for (int i = 0; i < raw.size(); i++) {
      Object[] nextRaw = raw.get(i);
      A nextStored = stored.get(i);
      Assert.assertEquals((int) (Integer) nextRaw[0], gf.getFactionID(nextStored));
      Assert.assertEquals(nextRaw[1], ga.getAttribute(nextStored));
      Assert.assertEquals((int) (Integer) nextRaw[2], gs.getStat(nextStored));
    }
  }

  private void verifyDataUpdate() throws Exception {
    // Retrieve all stored faction stats and victory points data
    List<FactionKillStat> storedKills = AbstractESIRefSync.retrieveAll(testTime, (long contid, AttributeSelector at) ->
        FactionKillStat.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));
    List<FactionVictoryPointStat> storedVPs = AbstractESIRefSync.retrieveAll(testTime, (long contid, AttributeSelector at) ->
        FactionVictoryPointStat.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(factionLeaderboardKillsTestData.length, storedKills.size());
    Assert.assertEquals(factionLeaderboardVPsTestData.length, storedVPs.size());

    // Check data
    verifyMatchingData(
        Arrays.stream(factionLeaderboardKillsTestData)
              .filter(x -> x[1] == StatAttribute.TOTAL)
              .collect(Collectors.toList()),
        storedKills.stream()
                   .filter(x -> x.getAttribute() == StatAttribute.TOTAL)
                   .collect(Collectors.toList()),
        FactionKillStat::getFactionID,
        AbstractKillStat::getAttribute,
        AbstractKillStat::getKills);
    verifyMatchingData(
        Arrays.stream(factionLeaderboardKillsTestData)
              .filter(x -> x[1] == StatAttribute.YESTERDAY)
              .collect(Collectors.toList()),
        storedKills.stream()
                   .filter(x -> x.getAttribute() == StatAttribute.YESTERDAY)
                   .collect(Collectors.toList()),
        FactionKillStat::getFactionID,
        AbstractKillStat::getAttribute,
        AbstractKillStat::getKills);
    verifyMatchingData(
        Arrays.stream(factionLeaderboardKillsTestData)
              .filter(x -> x[1] == StatAttribute.LAST_WEEK)
              .collect(Collectors.toList()),
        storedKills.stream()
                   .filter(x -> x.getAttribute() == StatAttribute.LAST_WEEK)
                   .collect(Collectors.toList()),
        FactionKillStat::getFactionID,
        AbstractKillStat::getAttribute,
        AbstractKillStat::getKills);
    verifyMatchingData(
        Arrays.stream(factionLeaderboardVPsTestData)
              .filter(x -> x[1] == StatAttribute.TOTAL)
              .collect(Collectors.toList()),
        storedVPs.stream()
                 .filter(x -> x.getAttribute() == StatAttribute.TOTAL)
                 .collect(Collectors.toList()),
        FactionVictoryPointStat::getFactionID,
        AbstractVictoryPointStat::getAttribute,
        AbstractVictoryPointStat::getVictoryPoints);
    verifyMatchingData(
        Arrays.stream(factionLeaderboardVPsTestData)
              .filter(x -> x[1] == StatAttribute.YESTERDAY)
              .collect(Collectors.toList()),
        storedVPs.stream()
                 .filter(x -> x.getAttribute() == StatAttribute.YESTERDAY)
                 .collect(Collectors.toList()),
        FactionVictoryPointStat::getFactionID,
        AbstractVictoryPointStat::getAttribute,
        AbstractVictoryPointStat::getVictoryPoints);
    verifyMatchingData(
        Arrays.stream(factionLeaderboardVPsTestData)
              .filter(x -> x[1] == StatAttribute.LAST_WEEK)
              .collect(Collectors.toList()),
        storedVPs.stream()
                 .filter(x -> x.getAttribute() == StatAttribute.LAST_WEEK)
                 .collect(Collectors.toList()),
        FactionVictoryPointStat::getFactionID,
        AbstractVictoryPointStat::getAttribute,
        AbstractVictoryPointStat::getVictoryPoints);
  }

  @Test
  public void testSyncUpdate() throws Exception {
    setupMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Perform the sync
    ESIFacWarFactionLeaderboardSync sync = new ESIFacWarFactionLeaderboardSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify updated properly
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_FW_FACTION_LEADERBOARD);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_FACTION_LEADERBOARD);
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
    int[] modifiedKillsFactionIDs = new int[factionLeaderboardKillsTestData.length];
    for (int i = 0; i < modifiedKillsFactionIDs.length; i++) {
      if (i % 2 == 0)
        modifiedKillsFactionIDs[i] = TestBase.getUniqueRandomInteger();
      else
        modifiedKillsFactionIDs[i] = (int) (Integer) factionLeaderboardKillsTestData[i][0];
    }
    int[] modifiedVPsFactionIDs = new int[factionLeaderboardVPsTestData.length];
    for (int i = 0; i < modifiedVPsFactionIDs.length; i++) {
      if (i % 2 == 0)
        modifiedVPsFactionIDs[i] = TestBase.getUniqueRandomInteger();
      else
        modifiedVPsFactionIDs[i] = (int) (Integer) factionLeaderboardVPsTestData[i][0];
    }
    // Populate existing stats
    for (int i = 0; i < factionLeaderboardKillsTestData.length; i++) {
      FactionKillStat newStat = new FactionKillStat((StatAttribute) factionLeaderboardKillsTestData[i][1],
                                                    (Integer) factionLeaderboardKillsTestData[i][2] + 1,
                                                    modifiedKillsFactionIDs[i]);
      newStat.setup(testTime - 1);
      RefCachedData.update(newStat);
    }
    for (int i = 0; i < factionLeaderboardVPsTestData.length; i++) {
      FactionVictoryPointStat newStat = new FactionVictoryPointStat((StatAttribute) factionLeaderboardVPsTestData[i][1],
                                                                    (Integer) factionLeaderboardVPsTestData[i][2] + 1,
                                                                    modifiedVPsFactionIDs[i]);
      newStat.setup(testTime - 1);
      RefCachedData.update(newStat);
    }

    // Perform the sync
    ESIFacWarFactionLeaderboardSync sync = new ESIFacWarFactionLeaderboardSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify old objects were evolved properly
    List<FactionKillStat> oldKills = AbstractESIRefSync.retrieveAll(testTime - 1, (long contid, AttributeSelector at) ->
        FactionKillStat.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));
    List<FactionVictoryPointStat> oldVPs = AbstractESIRefSync.retrieveAll(testTime - 1, (long contid, AttributeSelector at) ->
        FactionVictoryPointStat.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(factionLeaderboardKillsTestData.length, oldKills.size());
    Assert.assertEquals(factionLeaderboardVPsTestData.length, oldVPs.size());

    // Check old wars data
    for (int i = 0; i < factionLeaderboardKillsTestData.length; i++) {
      FactionKillStat nextStat = oldKills.get(i);
      Assert.assertEquals(testTime, nextStat.getLifeEnd());
      Assert.assertEquals(modifiedKillsFactionIDs[i], nextStat.getFactionID());
      Assert.assertEquals(factionLeaderboardKillsTestData[i][1], nextStat.getAttribute());
      Assert.assertEquals((Integer) factionLeaderboardKillsTestData[i][2] + 1, nextStat.getKills());
    }
    for (int i = 0; i < factionLeaderboardVPsTestData.length; i++) {
      FactionVictoryPointStat nextStat = oldVPs.get(i);
      Assert.assertEquals(testTime, nextStat.getLifeEnd());
      Assert.assertEquals(modifiedVPsFactionIDs[i], nextStat.getFactionID());
      Assert.assertEquals(factionLeaderboardVPsTestData[i][1], nextStat.getAttribute());
      Assert.assertEquals((Integer) factionLeaderboardVPsTestData[i][2] + 1, nextStat.getVictoryPoints());
    }

    // Verify updates which will also verify that all old alliances were properly end of lifed
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_FW_FACTION_LEADERBOARD);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_FACTION_LEADERBOARD);
    long schedTime = (new DateTime(2017, 12, 21, 12, 0, 0, DateTimeZone.UTC)).getMillis();
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

}
