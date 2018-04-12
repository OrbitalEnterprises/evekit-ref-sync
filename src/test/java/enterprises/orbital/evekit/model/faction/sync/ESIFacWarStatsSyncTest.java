package enterprises.orbital.evekit.model.faction.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.FactionWarfareApi;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.GetFwStats200Ok;
import enterprises.orbital.eve.esi.client.model.GetFwStatsKills;
import enterprises.orbital.eve.esi.client.model.GetFwStatsVictoryPoints;
import enterprises.orbital.evekit.TestBase;
import enterprises.orbital.evekit.account.EveKitRefDataProvider;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.faction.FactionStats;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ESIFacWarStatsSyncTest extends RefTestBase {

  // Local mocks and other objects
  private ESIRefClientProvider mockServer;
  private FactionWarfareApi mockEndpoint;
  private long testTime = 1238L;

  private static Object[][] factionWarStatsTestData;

  static {
    // Faction war stats test data
    // 0 int factionID;
    // 1 int killsLastWeek;
    // 2 int killsTotal;
    // 3 int killsYesterday;
    // 4 int pilots;
    // 5 int systemsControlled;
    // 6 int victoryPointsLastWeek;
    // 7 int victoryPointsTotal;
    // 8 int victoryPointsYesterday;
    int size = 20 + TestBase.getRandomInt(20);
    factionWarStatsTestData = new Object[size][9];
    for (int i = 0; i < size; i++) {
      factionWarStatsTestData[i][0] = TestBase.getUniqueRandomInteger();
      factionWarStatsTestData[i][1] = TestBase.getRandomInt();
      factionWarStatsTestData[i][2] = TestBase.getRandomInt();
      factionWarStatsTestData[i][3] = TestBase.getRandomInt();
      factionWarStatsTestData[i][4] = TestBase.getRandomInt();
      factionWarStatsTestData[i][5] = TestBase.getRandomInt();
      factionWarStatsTestData[i][6] = TestBase.getRandomInt();
      factionWarStatsTestData[i][7] = TestBase.getRandomInt();
      factionWarStatsTestData[i][8] = TestBase.getRandomInt();
    }
  }

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();

    // Prepare a test sync tracker
    ESIRefEndpointSyncTracker.getOrCreateUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_STATS, 1234L, null);

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
                                                .createQuery("DELETE FROM FactionStats")
                                                .executeUpdate();
                         });
    OrbitalProperties.setTimeGenerator(null);
    super.teardown();
  }

  // Mock up server interface
  private void setupMock() throws Exception {
    // Setup endpoint mock
    mockEndpoint = EasyMock.createMock(FactionWarfareApi.class);

    // Setup faction stats call
    List<GetFwStats200Ok> factionStatsList = new ArrayList<>();
    for (Object[] factionData : factionWarStatsTestData) {
      GetFwStats200Ok next = new GetFwStats200Ok();
      next.setFactionId((Integer) factionData[0]);
      next.setPilots((Integer) factionData[4]);
      next.setSystemsControlled((Integer) factionData[5]);
      GetFwStatsKills killStat = new GetFwStatsKills();
      killStat.setLastWeek((Integer) factionData[1]);
      killStat.setTotal((Integer) factionData[2]);
      killStat.setYesterday((Integer) factionData[3]);
      next.setKills(killStat);
      GetFwStatsVictoryPoints vpStat = new GetFwStatsVictoryPoints();
      vpStat.setLastWeek((Integer) factionData[6]);
      vpStat.setTotal((Integer) factionData[7]);
      vpStat.setYesterday((Integer) factionData[8]);
      next.setVictoryPoints(vpStat);
      factionStatsList.add(next);
    }
    Map<String, List<String>> headers = createHeaders("Expires", "Thu, 21 Dec 2017 12:00:00 GMT");
    ApiResponse<List<GetFwStats200Ok>> factionStatsListResponse = new ApiResponse<>(200, headers, factionStatsList);
    EasyMock.expect(mockEndpoint.getFwStatsWithHttpInfo(null, null, null))
            .andReturn(factionStatsListResponse);

    // Finally, setup client provider mock
    mockServer = EasyMock.createMock(ESIRefClientProvider.class);
    EasyMock.expect(mockServer.getFactionWarfareApi())
            .andReturn(mockEndpoint)
            .anyTimes();
  }

  private void verifyDataUpdate() throws Exception {
    // Retrieve all stored faction stats data
    List<FactionStats> storedStats = AbstractESIRefSync.retrieveAll(testTime, (long contid, AttributeSelector at) ->
        FactionStats.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(factionWarStatsTestData.length, storedStats.size());

    // Check faction stats data
    for (int i = 0; i < factionWarStatsTestData.length; i++) {
      FactionStats nextStat = storedStats.get(i);
      Assert.assertEquals((int) (Integer) factionWarStatsTestData[i][0], nextStat.getFactionID());
      Assert.assertEquals((int) (Integer) factionWarStatsTestData[i][1], nextStat.getKillsLastWeek());
      Assert.assertEquals((int) (Integer) factionWarStatsTestData[i][2], nextStat.getKillsTotal());
      Assert.assertEquals((int) (Integer) factionWarStatsTestData[i][3], nextStat.getKillsYesterday());
      Assert.assertEquals((int) (Integer) factionWarStatsTestData[i][4], nextStat.getPilots());
      Assert.assertEquals((int) (Integer) factionWarStatsTestData[i][5], nextStat.getSystemsControlled());
      Assert.assertEquals((int) (Integer) factionWarStatsTestData[i][6], nextStat.getVictoryPointsLastWeek());
      Assert.assertEquals((int) (Integer) factionWarStatsTestData[i][7], nextStat.getVictoryPointsTotal());
      Assert.assertEquals((int) (Integer) factionWarStatsTestData[i][8], nextStat.getVictoryPointsYesterday());
    }
  }

  @Test
  public void testSyncUpdate() throws Exception {
    setupMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Perform the sync
    ESIFacWarStatsSync sync = new ESIFacWarStatsSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify updated properly
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_FW_STATS);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_STATS);
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
    int[] modifiedFactionIDs = new int[factionWarStatsTestData.length];
    for (int i = 0; i < modifiedFactionIDs.length; i++) {
      if (i % 2 == 0)
        modifiedFactionIDs[i] = TestBase.getUniqueRandomInteger();
      else
        modifiedFactionIDs[i] = (int) (Integer) factionWarStatsTestData[i][0];
    }
    // Populate existing stats
    for (int i = 0; i < factionWarStatsTestData.length; i++) {
      FactionStats newStat = new FactionStats(modifiedFactionIDs[i],
                                              (Integer) factionWarStatsTestData[i][1] + 1,
                                              (Integer) factionWarStatsTestData[i][2] + 1,
                                              (Integer) factionWarStatsTestData[i][3] + 1,
                                              (Integer) factionWarStatsTestData[i][4] + 1,
                                              (Integer) factionWarStatsTestData[i][5] + 1,
                                              (Integer) factionWarStatsTestData[i][6] + 1,
                                              (Integer) factionWarStatsTestData[i][7] + 1,
                                              (Integer) factionWarStatsTestData[i][8] + 1);
      newStat.setup(testTime - 1);
      RefCachedData.update(newStat);
    }

    // Perform the sync
    ESIFacWarStatsSync sync = new ESIFacWarStatsSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify old objects were evolved properly
    List<FactionStats> oldStats = AbstractESIRefSync.retrieveAll(testTime - 1, (long contid, AttributeSelector at) ->
        FactionStats.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(factionWarStatsTestData.length, oldStats.size());

    // Check old stats data
    for (int i = 0; i < factionWarStatsTestData.length; i++) {
      FactionStats nextMap = oldStats.get(i);
      Assert.assertEquals(testTime, nextMap.getLifeEnd());
      Assert.assertEquals(modifiedFactionIDs[i], nextMap.getFactionID());
      Assert.assertEquals((Integer) factionWarStatsTestData[i][1] + 1, nextMap.getKillsLastWeek());
      Assert.assertEquals((Integer) factionWarStatsTestData[i][2] + 1, nextMap.getKillsTotal());
      Assert.assertEquals((Integer) factionWarStatsTestData[i][3] + 1, nextMap.getKillsYesterday());
      Assert.assertEquals((Integer) factionWarStatsTestData[i][4] + 1, nextMap.getPilots());
      Assert.assertEquals((Integer) factionWarStatsTestData[i][5] + 1, nextMap.getSystemsControlled());
      Assert.assertEquals((Integer) factionWarStatsTestData[i][6] + 1, nextMap.getVictoryPointsLastWeek());
      Assert.assertEquals((Integer) factionWarStatsTestData[i][7] + 1, nextMap.getVictoryPointsTotal());
      Assert.assertEquals((Integer) factionWarStatsTestData[i][8] + 1, nextMap.getVictoryPointsYesterday());
    }

    // Verify updates which will also verify that all old alliances were properly end of lifed
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_FW_STATS);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_STATS);
    long schedTime = (new DateTime(2017, 12, 21, 12, 0, 0, DateTimeZone.UTC)).getMillis();
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

}
