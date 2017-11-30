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
import enterprises.orbital.evekit.model.map.MapKill;
import enterprises.orbital.evexmlapi.map.IMapAPI;
import enterprises.orbital.evexmlapi.map.IMapKill;
import enterprises.orbital.evexmlapi.map.ISystemKills;

public class MapKillSyncTest extends RefTestBase {

  // Local mocks and other objects
  long                testDate;
  long                prevDate;
  RefData             container;
  RefSyncTracker      tracker;
  RefSynchronizerUtil syncUtil;
  IMapAPI             mockServer;

  static Object[][]   killTestData;

  static {
    // MapKill test data
    // 0 int factionKills
    // 1 int podKills
    // 2 int shipKills
    // 3 int solarSystemID
    int size = 20 + TestBase.getRandomInt(20);
    killTestData = new Object[size][4];
    for (int i = 0; i < size; i++) {
      killTestData[i][0] = TestBase.getRandomInt();
      killTestData[i][1] = TestBase.getRandomInt();
      killTestData[i][2] = TestBase.getRandomInt();
      killTestData[i][3] = TestBase.getUniqueRandomInteger();
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
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM MapKill").executeUpdate();
      }
    });

    super.teardown();
  }

  // Mock up server interface
  public void setupOkMock() throws Exception {
    mockServer = EasyMock.createMock(IMapAPI.class);
    final List<ISystemKills> kills = new ArrayList<>();
    for (int i = 0; i < killTestData.length; i++) {
      final Object[] data = killTestData[i];
      kills.add(new ISystemKills() {

        @Override
        public int getFactionKills() {
          return (Integer) data[0];
        }

        @Override
        public int getPodKills() {
          return (Integer) data[1];
        }

        @Override
        public int getShipKills() {
          return (Integer) data[2];
        }

        @Override
        public int getSolarSystemID() {
          return (Integer) data[3];
        }

      });
    }
    IMapKill killData = new IMapKill() {

      @Override
      public Date getDataTime() {
        return new Date(testDate);
      }

      @Override
      public Collection<ISystemKills> getKills() {
        return kills;
      }

    };
    EasyMock.expect(mockServer.requestKills()).andReturn(killData);
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
    SyncStatus syncOutcome = MapKillSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<MapKill> storedKills = retrieveAll(new BatchRetriever<MapKill>() {

      @Override
      public List<MapKill> getNextBatch(
                                        List<MapKill> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return MapKill.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(killTestData.length, storedKills.size());
    for (int i = 0; i < killTestData.length; i++) {
      MapKill nextKill = storedKills.get(i);
      Assert.assertEquals((int) (Integer) killTestData[i][0], nextKill.getFactionKills());
      Assert.assertEquals((int) (Integer) killTestData[i][1], nextKill.getPodKills());
      Assert.assertEquals((int) (Integer) killTestData[i][2], nextKill.getShipKills());
      Assert.assertEquals((int) (Integer) killTestData[i][3], nextKill.getSolarSystemID());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getMapKillExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getMapKillStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getMapKillDetail());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < killTestData.length; i++) {
      // Make half the existing data have unseen solar system IDs. These items should be removed during the sync
      // since they will not be returned from the API.
      MapKill newKill = new MapKill(
          (Integer) killTestData[i][0] + 1, (Integer) killTestData[i][1] + 1, (Integer) killTestData[i][2] + 1,
          i % 2 == 0 ? TestBase.getUniqueRandomInteger() : (Integer) killTestData[i][3]);
      newKill.setup(testTime - 1);
      RefCachedData.updateData(newKill);
    }

    // Perform the sync
    SyncStatus syncOutcome = MapKillSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<MapKill> storedKills = retrieveAll(new BatchRetriever<MapKill>() {

      @Override
      public List<MapKill> getNextBatch(
                                        List<MapKill> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return MapKill.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(killTestData.length, storedKills.size());
    for (int i = 0; i < killTestData.length; i++) {
      MapKill nextKill = storedKills.get(i);
      Assert.assertEquals((int) (Integer) killTestData[i][0], nextKill.getFactionKills());
      Assert.assertEquals((int) (Integer) killTestData[i][1], nextKill.getPodKills());
      Assert.assertEquals((int) (Integer) killTestData[i][2], nextKill.getShipKills());
      Assert.assertEquals((int) (Integer) killTestData[i][3], nextKill.getSolarSystemID());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getMapKillExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getMapKillStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getMapKillDetail());
  }

  // Test skips update when already updated
  @Test
  public void testSyncUpdateSkip() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < killTestData.length; i++) {
      MapKill newKill = new MapKill(
          (Integer) killTestData[i][0] + 1, (Integer) killTestData[i][1] + 1, (Integer) killTestData[i][2] + 1, (Integer) killTestData[i][3] + 1);
      newKill.setup(testTime - 1);
      RefCachedData.updateData(newKill);
    }

    // Set the tracker as already updated and populate the container
    tracker.setMapKillStatus(SyncState.UPDATED);
    tracker.setMapKillDetail(null);
    tracker = RefSyncTracker.updateTracker(tracker);
    container.setMapKillExpiry(prevDate);
    container = RefCachedData.updateData(container);

    // Perform the sync
    SyncStatus syncOutcome = MapKillSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    // Skip the verify here since the calls should never be made

    // Verify unchanged
    List<MapKill> storedKills = retrieveAll(new BatchRetriever<MapKill>() {

      @Override
      public List<MapKill> getNextBatch(
                                        List<MapKill> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return MapKill.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(killTestData.length, storedKills.size());
    for (int i = 0; i < killTestData.length; i++) {
      MapKill nextKill = storedKills.get(i);
      Assert.assertEquals((Integer) killTestData[i][0] + 1, nextKill.getFactionKills());
      Assert.assertEquals((Integer) killTestData[i][1] + 1, nextKill.getPodKills());
      Assert.assertEquals((Integer) killTestData[i][2] + 1, nextKill.getShipKills());
      Assert.assertEquals((Integer) killTestData[i][3] + 1, nextKill.getSolarSystemID());
    }

    // Verify tracker and container unchanged
    Assert.assertEquals(prevDate, RefData.getRefData().getMapKillExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getMapKillStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getMapKillDetail());
  }

}
