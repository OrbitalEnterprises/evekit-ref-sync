package enterprises.orbital.evekit.model.sov.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.SovereigntyApi;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.GetSovereigntyStructures200Ok;
import enterprises.orbital.evekit.TestBase;
import enterprises.orbital.evekit.account.EveKitRefDataProvider;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.sov.SovereigntyStructure;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class ESISovereigntyStructureSyncTest extends RefTestBase {

  // Local mocks and other objects
  private ESIRefClientProvider mockServer;
  private SovereigntyApi mockEndpoint;
  private long testTime = 1238L;

  private static Object[][] sovereigntyStructureTestData;

  static {
    // Sovereignty structure test data
    // 0 int allianceID;
    // 1 int systemID
    // 2 long structureID
    // 3 int structureTypeID
    // 4 float vulnerabilityOccupancyLevel
    // 5 long vulnerableStartTime
    // 6 long vulnerableEndTime
    int size = 20 + TestBase.getRandomInt(20);
    sovereigntyStructureTestData = new Object[size][7];
    for (int i = 0; i < size; i++) {
      sovereigntyStructureTestData[i][0] = TestBase.getRandomInt();
      sovereigntyStructureTestData[i][1] = TestBase.getRandomInt();
      sovereigntyStructureTestData[i][2] = TestBase.getUniqueRandomLong();
      sovereigntyStructureTestData[i][3] = TestBase.getRandomInt();
      sovereigntyStructureTestData[i][4] = (float) TestBase.getRandomDouble(1000);
      sovereigntyStructureTestData[i][5] = TestBase.getRandomLong();
      sovereigntyStructureTestData[i][6] = TestBase.getRandomLong();
    }
  }

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();

    // Prepare a test sync tracker
    ESIRefEndpointSyncTracker.getOrCreateUnfinishedTracker(ESIRefSyncEndpoint.REF_SOV_STRUCTURE, 1234L, null);

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
                                                .createQuery("DELETE FROM SovereigntyStructure")
                                                .executeUpdate();
                         });
    OrbitalProperties.setTimeGenerator(null);
    super.teardown();
  }

  // Mock up server interface
  private void setupMock() throws Exception {
    // Setup Sovereignty endpoint mock
    mockEndpoint = EasyMock.createMock(SovereigntyApi.class);

    // Setup sovereignty structure list call
    List<GetSovereigntyStructures200Ok> sovereigntyStructureList = new ArrayList<>();
    for (Object[] sructureData : sovereigntyStructureTestData) {
      GetSovereigntyStructures200Ok next = new GetSovereigntyStructures200Ok();
      next.setAllianceId((Integer) sructureData[0]);
      next.setSolarSystemId((Integer) sructureData[1]);
      next.setStructureId((Long) sructureData[2]);
      next.setStructureTypeId((Integer) sructureData[3]);
      next.setVulnerabilityOccupancyLevel((Float) sructureData[4]);
      next.setVulnerableStartTime(new DateTime(new Date((Long) sructureData[5])));
      next.setVulnerableEndTime(new DateTime(new Date((Long) sructureData[6])));
      sovereigntyStructureList.add(next);
    }
    Map<String, List<String>> headers = createHeaders("Expires", "Thu, 21 Dec 2017 12:00:00 GMT");
    ApiResponse<List<GetSovereigntyStructures200Ok>> sovereigntyStructureListResponse = new ApiResponse<>(200, headers, sovereigntyStructureList);
    EasyMock.expect(mockEndpoint.getSovereigntyStructuresWithHttpInfo(null, null))
            .andReturn(sovereigntyStructureListResponse);

    // Finally, setup client provider mock
    mockServer = EasyMock.createMock(ESIRefClientProvider.class);
    EasyMock.expect(mockServer.getSovereigntyApi())
            .andReturn(mockEndpoint)
            .anyTimes();
  }

  private void verifyDataUpdate() throws Exception {
    // Retrieve all stored sovereignty structure data
    List<SovereigntyStructure> storedStructures = AbstractESIRefSync.retrieveAll(testTime, (long contid, AttributeSelector at) ->
        SovereigntyStructure.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(sovereigntyStructureTestData.length, storedStructures.size());

    // Check sovereignty structure data
    for (int i = 0; i < sovereigntyStructureTestData.length; i++) {
      SovereigntyStructure nextStructure = storedStructures.get(i);
      Assert.assertEquals((int) (Integer) sovereigntyStructureTestData[i][0], nextStructure.getAllianceID());
      Assert.assertEquals((int) (Integer) sovereigntyStructureTestData[i][1], nextStructure.getSystemID());
      Assert.assertEquals((long) (Long) sovereigntyStructureTestData[i][2], nextStructure.getStructureID());
      Assert.assertEquals((int) (Integer) sovereigntyStructureTestData[i][3], nextStructure.getStructureTypeID());
      Assert.assertEquals((Float) sovereigntyStructureTestData[i][4], nextStructure.getVulnerabilityOccupancyLevel(), 0.001F);
      Assert.assertEquals((long) (Long) sovereigntyStructureTestData[i][5], nextStructure.getVulnerableStartTime());
      Assert.assertEquals((long) (Long) sovereigntyStructureTestData[i][6], nextStructure.getVulnerableEndTime());
    }
  }

  @Test
  public void testSyncUpdate() throws Exception {
    setupMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Perform the sync
    ESISovereigntyStructureSync sync = new ESISovereigntyStructureSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify updated properly
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_SOV_STRUCTURE);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_SOV_STRUCTURE);
    long schedTime = (new DateTime(2017, 12, 21, 12, 0, 0, DateTimeZone.UTC)).getMillis();
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Populate existing
    //
    // Half the structure entries we insert are purposely not present in the server data so we can test deletion.
    // Compute the modified structure IDs
    long[] modifiedStructureIDs = new long[sovereigntyStructureTestData.length];
    for (int i = 0; i < modifiedStructureIDs.length; i++) {
      if (i % 2 == 0)
        modifiedStructureIDs[i] = TestBase.getUniqueRandomLong();
      else
        modifiedStructureIDs[i] = (long) (Long) sovereigntyStructureTestData[i][2];
    }
    // Populate existing sovereignty structures
    for (int i = 0; i < sovereigntyStructureTestData.length; i++) {
      SovereigntyStructure newStructure = new SovereigntyStructure((Integer) sovereigntyStructureTestData[i][0] + 1,
                                                                   (Integer) sovereigntyStructureTestData[i][1] + 1,
                                                                   modifiedStructureIDs[i],
                                                                   (Integer) sovereigntyStructureTestData[i][3] + 1,
                                                                   (Float) sovereigntyStructureTestData[i][4] + 1.0F,
                                                                   (Long) sovereigntyStructureTestData[i][5] + 1,
                                                                   (Long) sovereigntyStructureTestData[i][6] + 1);
      newStructure.setup(testTime - 1);
      RefCachedData.update(newStructure);
    }

    // Perform the sync
    ESISovereigntyStructureSync sync = new ESISovereigntyStructureSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify old objects were evolved properly
    List<SovereigntyStructure> oldStructures = AbstractESIRefSync.retrieveAll(testTime - 1, (long contid, AttributeSelector at) ->
        SovereigntyStructure.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(sovereigntyStructureTestData.length, oldStructures.size());

    // Check old sovereignty structure data
    for (int i = 0; i < sovereigntyStructureTestData.length; i++) {
      SovereigntyStructure nextStructure = oldStructures.get(i);
      Assert.assertEquals(testTime, nextStructure.getLifeEnd());
      Assert.assertEquals(modifiedStructureIDs[i], nextStructure.getStructureID());
      Assert.assertEquals((Integer) sovereigntyStructureTestData[i][0] + 1, nextStructure.getAllianceID());
      Assert.assertEquals((Integer) sovereigntyStructureTestData[i][1] + 1, nextStructure.getSystemID());
      Assert.assertEquals((Integer) sovereigntyStructureTestData[i][3] + 1, nextStructure.getStructureTypeID());
      Assert.assertEquals((Float) sovereigntyStructureTestData[i][4] + 1.0F, nextStructure.getVulnerabilityOccupancyLevel(), 0.001);
      Assert.assertEquals((Long) sovereigntyStructureTestData[i][5] + 1, nextStructure.getVulnerableStartTime());
      Assert.assertEquals((Long) sovereigntyStructureTestData[i][6] + 1, nextStructure.getVulnerableEndTime());
    }

    // Verify updates which will also verify that all old alliances were properly end of lifed
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_SOV_STRUCTURE);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_SOV_STRUCTURE);
    long schedTime = (new DateTime(2017, 12, 21, 12, 0, 0, DateTimeZone.UTC)).getMillis();
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

}
