package enterprises.orbital.evekit.model.faction.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.FactionWarfareApi;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.GetFwWars200Ok;
import enterprises.orbital.evekit.TestBase;
import enterprises.orbital.evekit.account.EveKitRefDataProvider;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.faction.FactionWar;
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

public class ESIFacWarWarsSyncTest extends RefTestBase {

  // Local mocks and other objects
  private ESIClientProvider mockServer;
  private FactionWarfareApi mockEndpoint;
  private long testTime = 1238L;

  private static Object[][] factionWarWarsTestData;

  static {
    // Faction war stats test data
    // 0 int againstID;
    // 1 int factionID;
    int size = 20 + TestBase.getRandomInt(20);
    factionWarWarsTestData = new Object[size][2];
    for (int i = 0; i < size; i++) {
      factionWarWarsTestData[i][0] = TestBase.getUniqueRandomInteger();
      factionWarWarsTestData[i][1] = TestBase.getUniqueRandomInteger();
    }
  }

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();

    // Prepare a test sync tracker
    ESIRefEndpointSyncTracker.getOrCreateUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_WARS, 1234L);

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
                                                .createQuery("DELETE FROM FactionWar ")
                                                .executeUpdate();
                         });
    OrbitalProperties.setTimeGenerator(null);
    super.teardown();
  }

  // Mock up server interface
  private void setupMock() throws Exception {
    // Setup endpoint mock
    mockEndpoint = EasyMock.createMock(FactionWarfareApi.class);

    // Setup faction wars call
    List<GetFwWars200Ok> factionWarsList = new ArrayList<>();
    for (Object[] factionData : factionWarWarsTestData) {
      GetFwWars200Ok next = new GetFwWars200Ok();
      next.setAgainstId((Integer) factionData[0]);
      next.setFactionId((Integer) factionData[1]);
      factionWarsList.add(next);
    }
    Map<String, List<String>> headers = createHeaders("Expires", "Thu, 21 Dec 2017 12:00:00 GMT");
    ApiResponse<List<GetFwWars200Ok>> factionSystemsListResponse = new ApiResponse<>(200, headers, factionWarsList);
    EasyMock.expect(mockEndpoint.getFwWarsWithHttpInfo(null, null, null))
            .andReturn(factionSystemsListResponse);

    // Finally, setup client provider mock
    mockServer = EasyMock.createMock(ESIClientProvider.class);
    EasyMock.expect(mockServer.getFactionWarfareApi())
            .andReturn(mockEndpoint)
            .anyTimes();
  }

  private void verifyDataUpdate() throws Exception {
    // Retrieve all stored faction wars data
    List<FactionWar> storedWars = AbstractESIRefSync.retrieveAll(testTime, (long contid, AttributeSelector at) ->
        FactionWar.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(factionWarWarsTestData.length, storedWars.size());

    // Check faction wars data
    for (int i = 0; i < factionWarWarsTestData.length; i++) {
      FactionWar nextWar = storedWars.get(i);
      Assert.assertEquals((int) (Integer) factionWarWarsTestData[i][0], nextWar.getAgainstID());
      Assert.assertEquals((int) (Integer) factionWarWarsTestData[i][1], nextWar.getFactionID());
    }
  }

  @Test
  public void testSyncUpdate() throws Exception {
    setupMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Perform the sync
    ESIFacWarWarsSync sync = new ESIFacWarWarsSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify updated properly
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_FW_WARS);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_WARS);
    long schedTime = (new DateTime(2017, 12, 21, 12, 0, 0, DateTimeZone.UTC)).getMillis();
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Populate existing
    //
    // Half the wars entries we insert are purposely not present in the server data so we can test deletion.
    // Compute the modified system IDs
    int[] modifiedFactionIDs = new int[factionWarWarsTestData.length];
    for (int i = 0; i < modifiedFactionIDs.length; i++) {
      if (i % 2 == 0)
        modifiedFactionIDs[i] = TestBase.getUniqueRandomInteger();
      else
        modifiedFactionIDs[i] = (int) (Integer) factionWarWarsTestData[i][1];
    }
    // Populate existing wars
    for (int i = 0; i < factionWarWarsTestData.length; i++) {
      FactionWar newWar = new FactionWar((Integer) factionWarWarsTestData[i][0] + 1,
                                         modifiedFactionIDs[i]);
      newWar.setup(testTime - 1);
      RefCachedData.update(newWar);
    }

    // Perform the sync
    ESIFacWarWarsSync sync = new ESIFacWarWarsSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify old objects were evolved properly
    List<FactionWar> oldWars = AbstractESIRefSync.retrieveAll(testTime - 1, (long contid, AttributeSelector at) ->
        FactionWar.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(factionWarWarsTestData.length, oldWars.size());

    // Check old wars data
    for (int i = 0; i < factionWarWarsTestData.length; i++) {
      FactionWar nextWar = oldWars.get(i);
      Assert.assertEquals(testTime, nextWar.getLifeEnd());
      Assert.assertEquals(modifiedFactionIDs[i], nextWar.getFactionID());
      Assert.assertEquals((Integer) factionWarWarsTestData[i][0] + 1, nextWar.getAgainstID());
    }

    // Verify updates which will also verify that all old alliances were properly end of lifed
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_FW_WARS);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_WARS);
    long schedTime = (new DateTime(2017, 12, 21, 12, 0, 0, DateTimeZone.UTC)).getMillis();
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

}
