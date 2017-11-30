package enterprises.orbital.evekit.model.map.sync;

import java.text.DateFormat;
import java.util.ArrayList;
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
import enterprises.orbital.evekit.model.map.FactionWarSystem;
import enterprises.orbital.evexmlapi.map.IFacWarSystem;
import enterprises.orbital.evexmlapi.map.IMapAPI;

public class FacWarSystemsSyncTest extends RefTestBase {

  // Local mocks and other objects
  long                testDate;
  long                prevDate;
  RefData             container;
  RefSyncTracker      tracker;
  RefSynchronizerUtil syncUtil;
  IMapAPI             mockServer;

  static Object[][]   systemTestData;

  static {
    // FacWarSystem test data
    // 0 long occupyingFactionID
    // 1 String occupyingFactionName
    // 2 long owningFactionID
    // 3 String owningFactionName
    // 4 int solarSystemID
    // 5 String solarSystemName
    // 6 boolean contested
    int size = 20 + TestBase.getRandomInt(20);
    systemTestData = new Object[size][7];
    for (int i = 0; i < size; i++) {
      systemTestData[i][0] = TestBase.getRandomLong();
      systemTestData[i][1] = TestBase.getRandomText(50);
      systemTestData[i][2] = TestBase.getRandomLong();
      systemTestData[i][3] = TestBase.getRandomText(50);
      systemTestData[i][4] = TestBase.getUniqueRandomInteger();
      systemTestData[i][5] = TestBase.getRandomText(50);
      systemTestData[i][6] = TestBase.getRandomBoolean();
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
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM FactionWarSystem").executeUpdate();
      }
    });

    super.teardown();
  }

  // Mock up server interface
  public void setupOkMock() throws Exception {
    mockServer = EasyMock.createMock(IMapAPI.class);
    final List<IFacWarSystem> systems = new ArrayList<>();
    for (int i = 0; i < systemTestData.length; i++) {
      final Object[] data = systemTestData[i];
      systems.add(new IFacWarSystem() {

        @Override
        public long getOccupyingFactionID() {
          return (Long) data[0];
        }

        @Override
        public String getOccupyingFactionName() {
          return (String) data[1];
        }

        @Override
        public long getOwningFactionID() {
          return (Long) data[2];
        }

        @Override
        public String getOwningFactionName() {
          return (String) data[3];
        }

        @Override
        public int getSolarSystemID() {
          return (Integer) data[4];
        }

        @Override
        public String getSolarSystemName() {
          return (String) data[5];
        }

        @Override
        public boolean isContested() {
          return (Boolean) data[6];
        }

      });
    }
    EasyMock.expect(mockServer.requestFacWarSystems()).andReturn(systems);
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
    SyncStatus syncOutcome = FacWarSystemsSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<FactionWarSystem> storedSystems = retrieveAll(new BatchRetriever<FactionWarSystem>() {

      @Override
      public List<FactionWarSystem> getNextBatch(
                                                 List<FactionWarSystem> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return FactionWarSystem.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                            ANY_SELECTOR);
      }

    });
    Assert.assertEquals(systemTestData.length, storedSystems.size());
    for (int i = 0; i < systemTestData.length; i++) {
      FactionWarSystem nextSystem = storedSystems.get(i);
      Assert.assertEquals((long) (Long) systemTestData[i][0], nextSystem.getOccupyingFactionID());
      Assert.assertEquals(systemTestData[i][1], nextSystem.getOccupyingFactionName());
      Assert.assertEquals((long) (Long) systemTestData[i][2], nextSystem.getOwningFactionID());
      Assert.assertEquals(systemTestData[i][3], nextSystem.getOwningFactionName());
      Assert.assertEquals((int) (Integer) systemTestData[i][4], nextSystem.getSolarSystemID());
      Assert.assertEquals(systemTestData[i][5], nextSystem.getSolarSystemName());
      Assert.assertEquals((boolean) (Boolean) systemTestData[i][6], nextSystem.isContested());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getFacWarSystemsExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getFacWarSystemsStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getFacWarSystemsDetail());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < systemTestData.length; i++) {
      // Make half the existing data have unseen solar system IDs. These items should be removed during the sync
      // since they will not be returned from the API.
      FactionWarSystem newSystem = new FactionWarSystem(
          (Long) systemTestData[i][0] + 1, systemTestData[i][1] + "1", (Long) systemTestData[i][2] + 1, systemTestData[i][3] + "1",
          i % 2 == 0 ? TestBase.getUniqueRandomInteger() : (Integer) systemTestData[i][4], systemTestData[i][5] + "1", !(Boolean) systemTestData[i][6]);
      newSystem.setup(testTime - 1);
      RefCachedData.updateData(newSystem);
    }

    // Perform the sync
    SyncStatus syncOutcome = FacWarSystemsSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<FactionWarSystem> storedSystems = retrieveAll(new BatchRetriever<FactionWarSystem>() {

      @Override
      public List<FactionWarSystem> getNextBatch(
                                                 List<FactionWarSystem> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return FactionWarSystem.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                            ANY_SELECTOR);
      }

    });
    Assert.assertEquals(systemTestData.length, storedSystems.size());
    for (int i = 0; i < systemTestData.length; i++) {
      FactionWarSystem nextSystem = storedSystems.get(i);
      Assert.assertEquals((long) (Long) systemTestData[i][0], nextSystem.getOccupyingFactionID());
      Assert.assertEquals(systemTestData[i][1], nextSystem.getOccupyingFactionName());
      Assert.assertEquals((long) (Long) systemTestData[i][2], nextSystem.getOwningFactionID());
      Assert.assertEquals(systemTestData[i][3], nextSystem.getOwningFactionName());
      Assert.assertEquals((int) (Integer) systemTestData[i][4], nextSystem.getSolarSystemID());
      Assert.assertEquals(systemTestData[i][5], nextSystem.getSolarSystemName());
      Assert.assertEquals((boolean) (Boolean) systemTestData[i][6], nextSystem.isContested());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getFacWarSystemsExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getFacWarSystemsStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getFacWarSystemsDetail());
  }

  // Test skips update when already updated
  @Test
  public void testSyncUpdateSkip() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < systemTestData.length; i++) {
      FactionWarSystem newSystem = new FactionWarSystem(
          (Long) systemTestData[i][0] + 1, systemTestData[i][1] + "1", (Long) systemTestData[i][2] + 1, systemTestData[i][3] + "1",
          (Integer) systemTestData[i][4] + 1, systemTestData[i][5] + "1", !(Boolean) systemTestData[i][6]);
      newSystem.setup(testTime - 1);
      RefCachedData.updateData(newSystem);
    }

    // Set the tracker as already updated and populate the container
    tracker.setFacWarSystemsStatus(SyncState.UPDATED);
    tracker.setFacWarSystemsDetail(null);
    tracker = RefSyncTracker.updateTracker(tracker);
    container.setFacWarSystemsExpiry(prevDate);
    container = RefCachedData.updateData(container);

    // Perform the sync
    SyncStatus syncOutcome = FacWarSystemsSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    // Skip the verify here since the calls should never be made

    // Verify unchanged
    List<FactionWarSystem> storedSystems = retrieveAll(new BatchRetriever<FactionWarSystem>() {

      @Override
      public List<FactionWarSystem> getNextBatch(
                                                 List<FactionWarSystem> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return FactionWarSystem.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                            ANY_SELECTOR);
      }

    });
    Assert.assertEquals(systemTestData.length, storedSystems.size());
    for (int i = 0; i < systemTestData.length; i++) {
      FactionWarSystem nextSystem = storedSystems.get(i);
      Assert.assertEquals((Long) systemTestData[i][0] + 1, nextSystem.getOccupyingFactionID());
      Assert.assertEquals(systemTestData[i][1] + "1", nextSystem.getOccupyingFactionName());
      Assert.assertEquals((Long) systemTestData[i][2] + 1, nextSystem.getOwningFactionID());
      Assert.assertEquals(systemTestData[i][3] + "1", nextSystem.getOwningFactionName());
      Assert.assertEquals((Integer) systemTestData[i][4] + 1, nextSystem.getSolarSystemID());
      Assert.assertEquals(systemTestData[i][5] + "1", nextSystem.getSolarSystemName());
      Assert.assertEquals(!(boolean) (Boolean) systemTestData[i][6], nextSystem.isContested());
    }

    // Verify tracker and container unchanged
    Assert.assertEquals(prevDate, RefData.getRefData().getFacWarSystemsExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getFacWarSystemsStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getFacWarSystemsDetail());
  }

}
