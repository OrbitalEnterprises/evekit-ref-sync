package enterprises.orbital.evekit.model.server.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.StatusApi;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.GetStatusOk;
import enterprises.orbital.evekit.account.EveKitRefDataProvider;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.server.ServerStatus;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ESIServerStatusSyncTest extends RefTestBase {

  // Local mocks and other objects
  private ESIRefClientProvider mockServer;
  private StatusApi mockEndpoint;
  private long testTime = 1238L;

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();

    // Prepare a test sync tracker
    ESIRefEndpointSyncTracker.getOrCreateUnfinishedTracker(ESIRefSyncEndpoint.REF_SERVER_STATUS, 1234L, null);

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
                                                .createQuery("DELETE FROM ServerStatus")
                                                .executeUpdate();
                         });
    OrbitalProperties.setTimeGenerator(null);
    super.teardown();
  }

  // Mock up server interface
  private void setupOkMock() throws Exception {
    GetStatusOk gsok = new GetStatusOk();
    gsok.setPlayers(12341);
    gsok.setServerVersion("112233");
    gsok.setVip(false);
    gsok.setStartTime(DateTime.parse("2015-10-01"));
    Map<String, List<String>> headers = createHeaders("Expires", "Thu, 21 Dec 2017 12:00:00 GMT");
    ApiResponse<GetStatusOk> apir = new ApiResponse<>(200, headers, gsok);
    mockEndpoint = EasyMock.createMock(StatusApi.class);
    EasyMock.expect(mockEndpoint.getStatusWithHttpInfo(null, null, null, null))
            .andReturn(apir);
    mockServer = EasyMock.createMock(ESIRefClientProvider.class);
    EasyMock.expect(mockServer.getStatusApi())
            .andReturn(mockEndpoint);
  }

  @Test
  public void testSyncUpdate() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Perform the sync
    ESIServerStatusSync sync = new ESIServerStatusSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify updated properly
    ServerStatus result = ServerStatus.get(testTime);
    Assert.assertEquals(testTime, result.getLifeStart());
    Assert.assertEquals(Long.MAX_VALUE, result.getLifeEnd());
    Assert.assertEquals(12341, result.getOnlinePlayers());
    Assert.assertEquals(DateTime.parse("2015-10-01").getMillis(), result.getStartTime());
    Assert.assertEquals("112233", result.getServerVersion());
    Assert.assertFalse(result.isVip());

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_SERVER_STATUS);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_SERVER_STATUS);
    long schedTime = (new DateTime(2017, 12, 21, 12, 0, 0, DateTimeZone.UTC)).getMillis();
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Populate existing
    ServerStatus existing = new ServerStatus(8321, 8888L, "1.2.3", false);
    existing.setup(testTime - 1);
    RefCachedData.update(existing);

    // Perform the sync
    ESIServerStatusSync sync = new ESIServerStatusSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify old object was evolved properly
    ServerStatus result = ServerStatus.get(testTime - 1);
    Assert.assertEquals(testTime - 1, result.getLifeStart());
    Assert.assertEquals(testTime, result.getLifeEnd());
    Assert.assertEquals(8321, result.getOnlinePlayers());
    Assert.assertEquals(8888L, result.getStartTime());
    Assert.assertEquals("1.2.3", result.getServerVersion());
    Assert.assertFalse(result.isVip());

    // Verify updated properly
    result = ServerStatus.get(testTime);
    Assert.assertEquals(testTime, result.getLifeStart());
    Assert.assertEquals(Long.MAX_VALUE, result.getLifeEnd());
    Assert.assertEquals(12341, result.getOnlinePlayers());
    Assert.assertEquals(DateTime.parse("2015-10-01").getMillis(), result.getStartTime());
    Assert.assertEquals("112233", result.getServerVersion());
    Assert.assertFalse(result.isVip());

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_SERVER_STATUS);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_SERVER_STATUS);
    long schedTime = (new DateTime(2017, 12, 21, 12, 0, 0, DateTimeZone.UTC)).getMillis();
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

}
