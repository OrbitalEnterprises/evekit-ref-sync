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
import enterprises.orbital.evekit.model.map.Sovereignty;
import enterprises.orbital.evexmlapi.map.IMapAPI;
import enterprises.orbital.evexmlapi.map.ISovereignty;
import enterprises.orbital.evexmlapi.map.ISystemSovereignty;

public class SovereigntySyncTest extends RefTestBase {

  // Local mocks and other objects
  long                testDate;
  long                prevDate;
  RefData             container;
  RefSyncTracker      tracker;
  RefSynchronizerUtil syncUtil;
  IMapAPI             mockServer;

  static Object[][]   sovTestData;

  static {
    // Sovereignty test data
    // 0 long allianceID
    // 1 long corporationID
    // 2 long factionID
    // 3 int solarSystemID
    // 4 String solarSystemName
    int size = 20 + TestBase.getRandomInt(20);
    sovTestData = new Object[size][5];
    for (int i = 0; i < size; i++) {
      sovTestData[i][0] = TestBase.getRandomLong();
      sovTestData[i][1] = TestBase.getRandomLong();
      sovTestData[i][2] = TestBase.getRandomLong();
      sovTestData[i][3] = TestBase.getUniqueRandomInteger();
      sovTestData[i][4] = TestBase.getRandomText(50);
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
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM Sovereignty").executeUpdate();
      }
    });

    super.teardown();
  }

  // Mock up server interface
  public void setupOkMock() throws Exception {
    mockServer = EasyMock.createMock(IMapAPI.class);
    final List<ISystemSovereignty> sovs = new ArrayList<>();
    for (int i = 0; i < sovTestData.length; i++) {
      final Object[] data = sovTestData[i];
      sovs.add(new ISystemSovereignty() {

        @Override
        public long getAllianceID() {
          return (Long) data[0];
        }

        @Override
        public long getCorporationID() {
          return (Long) data[1];
        }

        @Override
        public long getFactionID() {
          return (Long) data[2];
        }

        @Override
        public int getSolarSystemID() {
          return (Integer) data[3];
        }

        @Override
        public String getSolarSystemName() {
          return (String) data[4];
        }
      });
    }
    ISovereignty sovData = new ISovereignty() {

      @Override
      public Date getDataTime() {
        return new Date(testDate);
      }

      @Override
      public Collection<ISystemSovereignty> getSystemSovereignty() {
        return sovs;
      }

    };

    EasyMock.expect(mockServer.requestSovereignty()).andReturn(sovData);
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
    SyncStatus syncOutcome = SovereigntySync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<Sovereignty> storedSovs = retrieveAll(new BatchRetriever<Sovereignty>() {

      @Override
      public List<Sovereignty> getNextBatch(
                                            List<Sovereignty> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return Sovereignty.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(sovTestData.length, storedSovs.size());
    for (int i = 0; i < sovTestData.length; i++) {
      Sovereignty nextSov = storedSovs.get(i);
      Assert.assertEquals((long) (Long) sovTestData[i][0], nextSov.getAllianceID());
      Assert.assertEquals((long) (Long) sovTestData[i][1], nextSov.getCorporationID());
      Assert.assertEquals((long) (Long) sovTestData[i][2], nextSov.getFactionID());
      Assert.assertEquals((int) (Integer) sovTestData[i][3], nextSov.getSolarSystemID());
      Assert.assertEquals(sovTestData[i][4], nextSov.getSolarSystemName());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getSovereigntyExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getSovereigntyStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getSovereigntyDetail());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < sovTestData.length; i++) {
      // Make half the existing data have unseen solar system IDs. These items should be removed during the sync
      // since they will not be returned from the API.
      Sovereignty newSov = new Sovereignty(
          (Long) sovTestData[i][0] + 1, (Long) sovTestData[i][1] + 1, (Long) sovTestData[i][2] + 1,
          i % 2 == 0 ? TestBase.getUniqueRandomInteger() : (Integer) sovTestData[i][3], sovTestData[i][4] + "1");
      newSov.setup(testTime - 1);
      RefCachedData.updateData(newSov);
    }

    // Perform the sync
    SyncStatus syncOutcome = SovereigntySync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<Sovereignty> storedSovs = retrieveAll(new BatchRetriever<Sovereignty>() {

      @Override
      public List<Sovereignty> getNextBatch(
                                            List<Sovereignty> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return Sovereignty.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(sovTestData.length, storedSovs.size());
    for (int i = 0; i < sovTestData.length; i++) {
      Sovereignty nextSov = storedSovs.get(i);
      Assert.assertEquals((long) (Long) sovTestData[i][0], nextSov.getAllianceID());
      Assert.assertEquals((long) (Long) sovTestData[i][1], nextSov.getCorporationID());
      Assert.assertEquals((long) (Long) sovTestData[i][2], nextSov.getFactionID());
      Assert.assertEquals((int) (Integer) sovTestData[i][3], nextSov.getSolarSystemID());
      Assert.assertEquals(sovTestData[i][4], nextSov.getSolarSystemName());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getSovereigntyExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getSovereigntyStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getSovereigntyDetail());
  }

  // Test skips update when already updated
  @Test
  public void testSyncUpdateSkip() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < sovTestData.length; i++) {
      // Make half the existing data have unseen solar system IDs. These items should be removed during the sync
      // since they will not be returned from the API.
      Sovereignty newSov = new Sovereignty(
          (Long) sovTestData[i][0] + 1, (Long) sovTestData[i][1] + 1, (Long) sovTestData[i][2] + 1, (Integer) sovTestData[i][3] + 1, sovTestData[i][4] + "1");
      newSov.setup(testTime - 1);
      RefCachedData.updateData(newSov);
    }

    // Set the tracker as already updated and populate the container
    tracker.setSovereigntyStatus(SyncState.UPDATED);
    tracker.setSovereigntyDetail(null);
    tracker = RefSyncTracker.updateTracker(tracker);
    container.setSovereigntyExpiry(prevDate);
    container = RefCachedData.updateData(container);

    // Perform the sync
    SyncStatus syncOutcome = SovereigntySync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    // Skip the verify here since the calls should never be made

    // Verify unchanged
    List<Sovereignty> storedSovs = retrieveAll(new BatchRetriever<Sovereignty>() {

      @Override
      public List<Sovereignty> getNextBatch(
                                            List<Sovereignty> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return Sovereignty.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(sovTestData.length, storedSovs.size());
    for (int i = 0; i < sovTestData.length; i++) {
      Sovereignty nextSov = storedSovs.get(i);
      Assert.assertEquals((Long) sovTestData[i][0] + 1, nextSov.getAllianceID());
      Assert.assertEquals((Long) sovTestData[i][1] + 1, nextSov.getCorporationID());
      Assert.assertEquals((Long) sovTestData[i][2] + 1, nextSov.getFactionID());
      Assert.assertEquals((Integer) sovTestData[i][3] + 1, nextSov.getSolarSystemID());
      Assert.assertEquals(sovTestData[i][4] + "1", nextSov.getSolarSystemName());
    }

    // Verify tracker and container unchanged
    Assert.assertEquals(prevDate, RefData.getRefData().getSovereigntyExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getSovereigntyStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getSovereigntyDetail());
  }

}
