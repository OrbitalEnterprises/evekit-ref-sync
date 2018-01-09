package enterprises.orbital.evekit.model.sov.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.SovereigntyApi;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.GetSovereigntyMap200Ok;
import enterprises.orbital.evekit.TestBase;
import enterprises.orbital.evekit.account.EveKitRefDataProvider;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.sov.SovereigntyMap;
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

public class ESISovereigntyMapSyncTest extends RefTestBase {

  // Local mocks and other objects
  private ESIRefClientProvider mockServer;
  private SovereigntyApi mockEndpoint;
  private long testTime = 1238L;

  private static Object[][] sovereigntyMapTestData;

  static {
    // Sovereignty map test data
    // 0 int allianceID;
    // 1 int corporationID
    // 2 int factionID
    // 3 int systemID
    int size = 20 + TestBase.getRandomInt(20);
    sovereigntyMapTestData = new Object[size][4];
    for (int i = 0; i < size; i++) {
      sovereigntyMapTestData[i][0] = TestBase.getRandomInt();
      sovereigntyMapTestData[i][1] = TestBase.getRandomInt();
      sovereigntyMapTestData[i][2] = TestBase.getRandomInt();
      sovereigntyMapTestData[i][3] = TestBase.getUniqueRandomInteger();
    }
  }

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();

    // Prepare a test sync tracker
    ESIRefEndpointSyncTracker.getOrCreateUnfinishedTracker(ESIRefSyncEndpoint.REF_SOV_MAP, 1234L);

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
                                                .createQuery("DELETE FROM SovereigntyMap")
                                                .executeUpdate();
                         });
    OrbitalProperties.setTimeGenerator(null);
    super.teardown();
  }

  // Mock up server interface
  private void setupMock() throws Exception {
    // Setup Sovereignty endpoint mock
    mockEndpoint = EasyMock.createMock(SovereigntyApi.class);

    // Setup sovereignty map list call
    List<GetSovereigntyMap200Ok> sovereigntyMapList = new ArrayList<>();
    for (Object[] mapData : sovereigntyMapTestData) {
      GetSovereigntyMap200Ok next = new GetSovereigntyMap200Ok();
      next.setAllianceId((Integer) mapData[0]);
      next.setCorporationId((Integer) mapData[1]);
      next.setFactionId((Integer) mapData[2]);
      next.setSystemId((Integer) mapData[3]);
      sovereigntyMapList.add(next);
    }
    Map<String, List<String>> headers = createHeaders("Expires", "Thu, 21 Dec 2017 12:00:00 GMT");
    ApiResponse<List<GetSovereigntyMap200Ok>> sovereigntyMapListResponse = new ApiResponse<>(200, headers, sovereigntyMapList);
    EasyMock.expect(mockEndpoint.getSovereigntyMapWithHttpInfo(null, null, null))
            .andReturn(sovereigntyMapListResponse);

    // Finally, setup client provider mock
    mockServer = EasyMock.createMock(ESIRefClientProvider.class);
    EasyMock.expect(mockServer.getSovereigntyApi())
            .andReturn(mockEndpoint)
            .anyTimes();
  }

  private void verifyDataUpdate() throws Exception {
    // Retrieve all stored sovereignty map data
    List<SovereigntyMap> storedMaps = AbstractESIRefSync.retrieveAll(testTime, (long contid, AttributeSelector at) ->
        SovereigntyMap.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(sovereigntyMapTestData.length, storedMaps.size());

    // Check sovereignty map data
    for (int i = 0; i < sovereigntyMapTestData.length; i++) {
      SovereigntyMap nextMap = storedMaps.get(i);
      Assert.assertEquals((int) (Integer) sovereigntyMapTestData[i][0], nextMap.getAllianceID());
      Assert.assertEquals((int) (Integer) sovereigntyMapTestData[i][1], nextMap.getCorporationID());
      Assert.assertEquals((int) (Integer) sovereigntyMapTestData[i][2], nextMap.getFactionID());
      Assert.assertEquals((int) (Integer) sovereigntyMapTestData[i][3], nextMap.getSystemID());
    }
  }

  @Test
  public void testSyncUpdate() throws Exception {
    setupMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Perform the sync
    ESISovereigntyMapSync sync = new ESISovereigntyMapSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify updated properly
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_SOV_MAP);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_SOV_MAP);
    long schedTime = (new DateTime(2017, 12, 21, 12, 0, 0, DateTimeZone.UTC)).getMillis();
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Populate existing
    //
    // Half the map entries we insert are purposely not present in the server data so we can test deletion.
    // Compute the modified systems IDs
    int[] modifiedSystemIDs = new int[sovereigntyMapTestData.length];
    for (int i = 0; i < modifiedSystemIDs.length; i++) {
      if (i % 2 == 0)
        modifiedSystemIDs[i] = TestBase.getUniqueRandomInteger();
      else
        modifiedSystemIDs[i] = (int) (Integer) sovereigntyMapTestData[i][3];
    }
    // Populate existing sovereignty maps
    for (int i = 0; i < sovereigntyMapTestData.length; i++) {
      SovereigntyMap newMap = new SovereigntyMap((Integer) sovereigntyMapTestData[i][0] + 1,
                                                 (Integer) sovereigntyMapTestData[i][1] + 1,
                                                 (Integer) sovereigntyMapTestData[i][2] + 1,
                                                 modifiedSystemIDs[i]);
      newMap.setup(testTime - 1);
      RefCachedData.update(newMap);
    }

    // Perform the sync
    ESISovereigntyMapSync sync = new ESISovereigntyMapSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify old objects were evolved properly
    List<SovereigntyMap> oldMaps = AbstractESIRefSync.retrieveAll(testTime - 1, (long contid, AttributeSelector at) ->
        SovereigntyMap.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(sovereigntyMapTestData.length, oldMaps.size());

    // Check old sovereignty map data
    for (int i = 0; i < sovereigntyMapTestData.length; i++) {
      SovereigntyMap nextMap = oldMaps.get(i);
      Assert.assertEquals(testTime, nextMap.getLifeEnd());
      Assert.assertEquals(modifiedSystemIDs[i], nextMap.getSystemID());
      Assert.assertEquals((Integer) sovereigntyMapTestData[i][0] + 1, nextMap.getAllianceID());
      Assert.assertEquals((Integer) sovereigntyMapTestData[i][1] + 1, nextMap.getCorporationID());
      Assert.assertEquals((Integer) sovereigntyMapTestData[i][2] + 1, nextMap.getFactionID());
    }

    // Verify updates which will also verify that all old alliances were properly end of lifed
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_SOV_MAP);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_SOV_MAP);
    long schedTime = (new DateTime(2017, 12, 21, 12, 0, 0, DateTimeZone.UTC)).getMillis();
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

}
