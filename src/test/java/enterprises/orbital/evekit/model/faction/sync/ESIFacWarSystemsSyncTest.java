package enterprises.orbital.evekit.model.faction.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.FactionWarfareApi;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.GetFwSystems200Ok;
import enterprises.orbital.evekit.TestBase;
import enterprises.orbital.evekit.account.EveKitRefDataProvider;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.faction.FactionWarSystem;
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

public class ESIFacWarSystemsSyncTest extends RefTestBase {

  // Local mocks and other objects
  private ESIRefClientProvider mockServer;
  private FactionWarfareApi mockEndpoint;
  private long testTime = 1238L;

  private static Object[][] factionWarSystemsTestData;

  static {
    // Faction war stats test data
    // 0 int occupyingFactionID;
    // 1 int owningFactionID;
    // 2 int solarSystemID;
    // 3 int victoryPoints;
    // 4 int victoryPointsThreshold;
    // 5 boolean contested;
    int size = 20 + TestBase.getRandomInt(20);
    factionWarSystemsTestData = new Object[size][6];
    for (int i = 0; i < size; i++) {
      factionWarSystemsTestData[i][0] = TestBase.getRandomInt();
      factionWarSystemsTestData[i][1] = TestBase.getRandomInt();
      factionWarSystemsTestData[i][2] = TestBase.getUniqueRandomInteger();
      factionWarSystemsTestData[i][3] = TestBase.getRandomInt();
      factionWarSystemsTestData[i][4] = TestBase.getRandomInt();
      factionWarSystemsTestData[i][5] = TestBase.getRandomBoolean();
    }
  }

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();

    // Prepare a test sync tracker
    ESIRefEndpointSyncTracker.getOrCreateUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_SYSTEMS, 1234L, null);

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
                                                .createQuery("DELETE FROM FactionWarSystem ")
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
    List<GetFwSystems200Ok> factionSystemsList = new ArrayList<>();
    for (Object[] factionData : factionWarSystemsTestData) {
      GetFwSystems200Ok next = new GetFwSystems200Ok();
      next.setOccupierFactionId((Integer) factionData[0]);
      next.setOwnerFactionId((Integer) factionData[1]);
      next.setSolarSystemId((Integer) factionData[2]);
      next.setVictoryPoints((Integer) factionData[3]);
      next.setVictoryPointsThreshold((Integer) factionData[4]);
      next.setContested((Boolean) factionData[5]);
      factionSystemsList.add(next);
    }
    Map<String, List<String>> headers = createHeaders("Expires", "Thu, 21 Dec 2017 12:00:00 GMT");
    ApiResponse<List<GetFwSystems200Ok>> factionSystemsListResponse = new ApiResponse<>(200, headers, factionSystemsList);
    EasyMock.expect(mockEndpoint.getFwSystemsWithHttpInfo(null, null))
            .andReturn(factionSystemsListResponse);

    // Finally, setup client provider mock
    mockServer = EasyMock.createMock(ESIRefClientProvider.class);
    EasyMock.expect(mockServer.getFactionWarfareApi())
            .andReturn(mockEndpoint)
            .anyTimes();
  }

  private void verifyDataUpdate() throws Exception {
    // Retrieve all stored faction stats data
    List<FactionWarSystem> storedSystems = AbstractESIRefSync.retrieveAll(testTime, (long contid, AttributeSelector at) ->
        FactionWarSystem.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(factionWarSystemsTestData.length, storedSystems.size());

    // Check faction stats data
    for (int i = 0; i < factionWarSystemsTestData.length; i++) {
      FactionWarSystem nextSystem = storedSystems.get(i);
      Assert.assertEquals((int) (Integer) factionWarSystemsTestData[i][0], nextSystem.getOccupyingFactionID());
      Assert.assertEquals((int) (Integer) factionWarSystemsTestData[i][1], nextSystem.getOwningFactionID());
      Assert.assertEquals((int) (Integer) factionWarSystemsTestData[i][2], nextSystem.getSolarSystemID());
      Assert.assertEquals((int) (Integer) factionWarSystemsTestData[i][3], nextSystem.getVictoryPoints());
      Assert.assertEquals((int) (Integer) factionWarSystemsTestData[i][4], nextSystem.getVictoryPointsThreshold());
      Assert.assertEquals(factionWarSystemsTestData[i][5], nextSystem.isContested());
    }
  }

  @Test
  public void testSyncUpdate() throws Exception {
    setupMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Perform the sync
    ESIFacWarSystemsSync sync = new ESIFacWarSystemsSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify updated properly
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_FW_SYSTEMS);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_SYSTEMS);
    long schedTime = (new DateTime(2017, 12, 21, 12, 0, 0, DateTimeZone.UTC)).getMillis();
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Populate existing
    //
    // Half the systems entries we insert are purposely not present in the server data so we can test deletion.
    // Compute the modified system IDs
    int[] modifiedSystemIDs = new int[factionWarSystemsTestData.length];
    for (int i = 0; i < modifiedSystemIDs.length; i++) {
      if (i % 2 == 0)
        modifiedSystemIDs[i] = TestBase.getUniqueRandomInteger();
      else
        modifiedSystemIDs[i] = (int) (Integer) factionWarSystemsTestData[i][2];
    }
    // Populate existing systems
    for (int i = 0; i < factionWarSystemsTestData.length; i++) {
      FactionWarSystem newSystem = new FactionWarSystem((Integer) factionWarSystemsTestData[i][0] + 1,
                                                        (Integer) factionWarSystemsTestData[i][1] + 1,
                                                        modifiedSystemIDs[i],
                                                        (Integer) factionWarSystemsTestData[i][3] + 1,
                                                        (Integer) factionWarSystemsTestData[i][4] + 1,
                                                        !((Boolean) factionWarSystemsTestData[i][5]));
      newSystem.setup(testTime - 1);
      RefCachedData.update(newSystem);
    }

    // Perform the sync
    ESIFacWarSystemsSync sync = new ESIFacWarSystemsSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify old objects were evolved properly
    List<FactionWarSystem> oldSystems = AbstractESIRefSync.retrieveAll(testTime - 1, (long contid, AttributeSelector at) ->
        FactionWarSystem.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(factionWarSystemsTestData.length, oldSystems.size());

    // Check old systems data
    for (int i = 0; i < factionWarSystemsTestData.length; i++) {
      FactionWarSystem nextSystem = oldSystems.get(i);
      Assert.assertEquals(testTime, nextSystem.getLifeEnd());
      Assert.assertEquals(modifiedSystemIDs[i], nextSystem.getSolarSystemID());
      Assert.assertEquals((Integer) factionWarSystemsTestData[i][0] + 1, nextSystem.getOccupyingFactionID());
      Assert.assertEquals((Integer) factionWarSystemsTestData[i][1] + 1, nextSystem.getOwningFactionID());
      Assert.assertEquals((Integer) factionWarSystemsTestData[i][3] + 1, nextSystem.getVictoryPoints());
      Assert.assertEquals((Integer) factionWarSystemsTestData[i][4] + 1, nextSystem.getVictoryPointsThreshold());
      Assert.assertEquals(!((Boolean) factionWarSystemsTestData[i][5]), nextSystem.isContested());
    }

    // Verify updates which will also verify that all old alliances were properly end of lifed
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_FW_SYSTEMS);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_SYSTEMS);
    long schedTime = (new DateTime(2017, 12, 21, 12, 0, 0, DateTimeZone.UTC)).getMillis();
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

}
