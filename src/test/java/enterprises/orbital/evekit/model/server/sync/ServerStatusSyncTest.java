package enterprises.orbital.evekit.model.server.sync;

import java.text.DateFormat;
import java.util.Date;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import enterprises.orbital.db.ConnectionFactory.RunInVoidTransaction;
import enterprises.orbital.evekit.account.EveKitRefDataProvider;
import enterprises.orbital.evekit.model.RefCachedData;
import enterprises.orbital.evekit.model.RefData;
import enterprises.orbital.evekit.model.RefSyncTracker;
import enterprises.orbital.evekit.model.RefSynchronizerUtil;
import enterprises.orbital.evekit.model.RefSynchronizerUtil.SyncStatus;
import enterprises.orbital.evekit.model.RefTestBase;
import enterprises.orbital.evekit.model.SyncTracker.SyncState;
import enterprises.orbital.evekit.model.server.ServerStatus;
import enterprises.orbital.evexmlapi.svr.IServerAPI;
import enterprises.orbital.evexmlapi.svr.IServerStatus;

public class ServerStatusSyncTest extends RefTestBase {

  // Local mocks and other objects
  long                testDate;
  long                prevDate;
  RefData             container;
  RefSyncTracker      tracker;
  RefSynchronizerUtil syncUtil;
  IServerAPI          mockServer;

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();

    // Prepare a test user and sync account
    testDate = DateFormat.getDateInstance().parse("Nov 16, 2010").getTime();
    prevDate = DateFormat.getDateInstance().parse("Jan 10, 2009").getTime();

    // Prepare a test sync tracker
    tracker = RefSyncTracker.createOrGetUnfinishedTracker();

    // Prepare a container
    container = RefData.getOrCreateRefData();

    // Prepare the synchronizer util
    syncUtil = new RefSynchronizerUtil();
  }

  @Override
  @After
  public void teardown() throws Exception {
    // Cleanup RefTracker, RefData and test specific tables after each test
    EveKitRefDataProvider.getFactory().runTransaction(new RunInVoidTransaction() {
      @Override
      public void run() throws Exception {
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM ServerStatus").executeUpdate();
      }
    });

    super.teardown();
  }

  // Mock up server interface
  public void setupOkMock() throws Exception {
    mockServer = EasyMock.createMock(IServerAPI.class);
    IServerStatus mockServerStatus = new IServerStatus() {

      @Override
      public int getOnlinePlayers() {
        return 12341;
      }

      @Override
      public boolean isServerOpen() {
        return true;
      }

    };
    EasyMock.expect(mockServer.requestServerStatus()).andReturn(mockServerStatus);
    EasyMock.expect(mockServer.isError()).andReturn(false);
    EasyMock.expect(mockServer.getCachedUntil()).andReturn(new Date(testDate));
  }

  @Test
  public void testSyncUpdate() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;

    // Perform the sync
    SyncStatus syncOutcome = ServerStatusSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    ServerStatus result = ServerStatus.get(testTime);
    Assert.assertEquals(12341, result.getOnlinePlayers());
    Assert.assertTrue(result.isServerOpen());

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getServerStatusExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getServerStatusStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getServerStatusDetail());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;

    // Populate existing
    ServerStatus existing = new ServerStatus(8321, false);
    existing.setup(testTime);
    existing = RefCachedData.updateData(existing);

    // Perform the sync
    SyncStatus syncOutcome = ServerStatusSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    ServerStatus result = ServerStatus.get(testTime);
    Assert.assertEquals(12341, result.getOnlinePlayers());
    Assert.assertTrue(result.isServerOpen());

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getServerStatusExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getServerStatusStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getServerStatusDetail());
  }

  // Test skips update when already updated
  @Test
  public void testSyncUpdateSkip() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;

    // Populate an existing account balance
    ServerStatus existing = new ServerStatus(8321, false);
    existing.setup(testTime);
    existing = RefCachedData.updateData(existing);

    // Set the tracker as already updated and populate the container
    tracker.setServerStatusStatus(SyncState.UPDATED);
    tracker.setServerStatusDetail(null);
    tracker = RefSyncTracker.updateTracker(tracker);
    container.setServerStatusExpiry(prevDate);
    container = RefCachedData.updateData(container);

    // Perform the sync
    SyncStatus syncOutcome = ServerStatusSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    // Skip the verify here since the calls should never be made

    // Verify unchanged
    ServerStatus result = ServerStatus.get(testTime);
    Assert.assertEquals(8321, result.getOnlinePlayers());
    Assert.assertFalse(result.isServerOpen());

    // Verify tracker and container unchanged
    Assert.assertEquals(prevDate, RefData.getRefData().getServerStatusExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getServerStatusStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getServerStatusDetail());
  }

}
