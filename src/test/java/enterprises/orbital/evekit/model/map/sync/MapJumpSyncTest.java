package enterprises.orbital.evekit.model.map.sync;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import enterprises.orbital.db.ConnectionFactory.RunInVoidTransaction;
import enterprises.orbital.evekit.TestBase;
import enterprises.orbital.evekit.account.EveKitRefDataProvider;
import enterprises.orbital.evekit.model.AttributeSelector;
import enterprises.orbital.evekit.model.RefCachedData;
import enterprises.orbital.evekit.model.RefData;
import enterprises.orbital.evekit.model.RefSyncTracker;
import enterprises.orbital.evekit.model.RefSynchronizerUtil;
import enterprises.orbital.evekit.model.RefSynchronizerUtil.SyncStatus;
import enterprises.orbital.evekit.model.RefTestBase;
import enterprises.orbital.evekit.model.SyncTracker.SyncState;
import enterprises.orbital.evekit.model.map.MapJump;
import enterprises.orbital.evexmlapi.map.IJump;
import enterprises.orbital.evexmlapi.map.IMapAPI;
import enterprises.orbital.evexmlapi.map.IMapJump;

public class MapJumpSyncTest extends RefTestBase {

  // Local mocks and other objects
  long                testDate;
  long                prevDate;
  RefData             container;
  RefSyncTracker      tracker;
  RefSynchronizerUtil syncUtil;
  IMapAPI             mockServer;

  static Object[][]   jumpTestData;

  static {
    // MapJump test data
    // 0 int solarSystemID
    // 1 int shipJumps
    int size = 20 + TestBase.getRandomInt(20);
    jumpTestData = new Object[size][2];
    for (int i = 0; i < size; i++) {
      jumpTestData[i][0] = TestBase.getUniqueRandomInteger();
      jumpTestData[i][1] = TestBase.getRandomInt();
    }
  }

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
    // Cleanup
    EveKitRefDataProvider.getFactory().runTransaction(new RunInVoidTransaction() {
      @Override
      public void run() throws Exception {
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM MapJump").executeUpdate();
      }
    });

    super.teardown();
  }

  // Mock up server interface
  public void setupOkMock() throws Exception {
    mockServer = EasyMock.createMock(IMapAPI.class);
    final List<IJump> jumps = new ArrayList<>();
    for (int i = 0; i < jumpTestData.length; i++) {
      final Object[] data = jumpTestData[i];
      jumps.add(new IJump() {

        @Override
        public int getShipJumps() {
          return (Integer) data[1];
        }

        @Override
        public int getSolarSystemID() {
          return (Integer) data[0];
        }
      });
    }
    IMapJump jumpData = new IMapJump() {

      @Override
      public Date getDataTime() {
        return new Date(testDate);
      }

      @Override
      public Collection<IJump> getJumps() {
        return jumps;
      }

    };
    EasyMock.expect(mockServer.requestJumps()).andReturn(jumpData);
    EasyMock.expect(mockServer.isError()).andReturn(false);
    EasyMock.expect(mockServer.getCachedUntil()).andReturn(new Date(testDate));
  }

  @Test
  public void testSyncUpdate() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Perform the sync
    SyncStatus syncOutcome = MapJumpSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<MapJump> storedJumps = retrieveAll(new BatchRetriever<MapJump>() {

      @Override
      public List<MapJump> getNextBatch(
                                        List<MapJump> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return MapJump.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(jumpTestData.length, storedJumps.size());
    for (int i = 0; i < jumpTestData.length; i++) {
      MapJump nextJump = storedJumps.get(i);
      Assert.assertEquals((int) (Integer) jumpTestData[i][0], nextJump.getSolarSystemID());
      Assert.assertEquals((int) (Integer) jumpTestData[i][1], nextJump.getShipJumps());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getMapJumpExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getMapJumpStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getMapJumpDetail());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < jumpTestData.length; i++) {
      // Make half the existing data have unseen solar system IDs. These items should be removed during the sync
      // since they will not be returned from the API.
      MapJump newJump = new MapJump(i % 2 == 0 ? TestBase.getUniqueRandomInteger() : (Integer) jumpTestData[i][0], (Integer) jumpTestData[i][1] + 1);
      newJump.setup(testTime - 1);
      RefCachedData.updateData(newJump);
    }

    // Perform the sync
    SyncStatus syncOutcome = MapJumpSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<MapJump> storedJumps = retrieveAll(new BatchRetriever<MapJump>() {

      @Override
      public List<MapJump> getNextBatch(
                                        List<MapJump> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return MapJump.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(jumpTestData.length, storedJumps.size());
    for (int i = 0; i < jumpTestData.length; i++) {
      MapJump nextJump = storedJumps.get(i);
      Assert.assertEquals((int) (Integer) jumpTestData[i][0], nextJump.getSolarSystemID());
      Assert.assertEquals((int) (Integer) jumpTestData[i][1], nextJump.getShipJumps());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getMapJumpExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getMapJumpStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getMapJumpDetail());
  }

  // Test skips update when already updated
  @Test
  public void testSyncUpdateSkip() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < jumpTestData.length; i++) {
      MapJump newJump = new MapJump((Integer) jumpTestData[i][0] + 1, (Integer) jumpTestData[i][1] + 1);
      newJump.setup(testTime - 1);
      RefCachedData.updateData(newJump);
    }

    // Set the tracker as already updated and populate the container
    tracker.setMapJumpStatus(SyncState.UPDATED);
    tracker.setMapJumpDetail(null);
    tracker = RefSyncTracker.updateTracker(tracker);
    container.setMapJumpExpiry(prevDate);
    container = RefCachedData.updateData(container);

    // Perform the sync
    SyncStatus syncOutcome = MapJumpSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    // Skip the verify here since the calls should never be made

    // Verify unchanged
    List<MapJump> storedJumps = retrieveAll(new BatchRetriever<MapJump>() {

      @Override
      public List<MapJump> getNextBatch(
                                        List<MapJump> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return MapJump.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(jumpTestData.length, storedJumps.size());
    for (int i = 0; i < jumpTestData.length; i++) {
      MapJump nextJump = storedJumps.get(i);
      Assert.assertEquals((Integer) jumpTestData[i][0] + 1, nextJump.getSolarSystemID());
      Assert.assertEquals((Integer) jumpTestData[i][1] + 1, nextJump.getShipJumps());
    }

    // Verify tracker and container unchanged
    Assert.assertEquals(prevDate, RefData.getRefData().getMapJumpExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getMapJumpStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getMapJumpDetail());
  }

}
