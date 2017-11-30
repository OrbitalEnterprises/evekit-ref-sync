package enterprises.orbital.evekit.model.eve.sync;

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
import enterprises.orbital.evekit.model.eve.ConquerableStation;
import enterprises.orbital.evexmlapi.eve.IConquerableStation;
import enterprises.orbital.evexmlapi.eve.IEveAPI;

public class ConquerableStationsSyncTest extends RefTestBase {

  // Local mocks and other objects
  long                testDate;
  long                prevDate;
  RefData             container;
  RefSyncTracker      tracker;
  RefSynchronizerUtil syncUtil;
  IEveAPI             mockServer;

  static Object[][]   conqStationTestData;

  static {
    // Conquerable station test data
    // 0 long corporationID
    // 1 String corporationName
    // 2 long solarSystemID
    // 3 long stationID
    // 4 String stationName
    // 5 int stationTypeID
    // 6 long x
    // 7 long y
    // 8 long z
    int size = 20 + TestBase.getRandomInt(20);
    conqStationTestData = new Object[size][9];
    for (int i = 0; i < size; i++) {
      conqStationTestData[i][0] = TestBase.getRandomLong();
      conqStationTestData[i][1] = TestBase.getRandomText(50);
      conqStationTestData[i][2] = TestBase.getRandomLong();
      conqStationTestData[i][3] = TestBase.getUniqueRandomLong();
      conqStationTestData[i][4] = TestBase.getRandomText(50);
      conqStationTestData[i][5] = TestBase.getRandomInt();
      conqStationTestData[i][6] = TestBase.getRandomLong();
      conqStationTestData[i][7] = TestBase.getRandomLong();
      conqStationTestData[i][8] = TestBase.getRandomLong();
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
    // Cleanup RefTracker, RefData and test specific tables after each test
    EveKitRefDataProvider.getFactory().runTransaction(new RunInVoidTransaction() {
      @Override
      public void run() throws Exception {
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM ConquerableStation").executeUpdate();
      }
    });

    super.teardown();
  }

  // Mock up server interface
  public void setupOkMock() throws Exception {
    mockServer = EasyMock.createMock(IEveAPI.class);
    final List<IConquerableStation> stations = new ArrayList<>();
    for (int i = 0; i < conqStationTestData.length; i++) {
      final Object[] data = conqStationTestData[i];
      stations.add(new IConquerableStation() {

        @Override
        public long getCorporationID() {
          return (Long) data[0];
        }

        @Override
        public String getCorporationName() {
          return (String) data[1];
        }

        @Override
        public long getSolarSystemID() {
          return (Long) data[2];
        }

        @Override
        public long getStationID() {
          return (Long) data[3];
        }

        @Override
        public String getStationName() {
          return (String) data[4];
        }

        @Override
        public int getStationTypeID() {
          return (Integer) data[5];
        }

        @Override
        public long getX() {
          return (Long) data[6];
        }

        @Override
        public long getY() {
          return (Long) data[7];
        }

        @Override
        public long getZ() {
          return (Long) data[8];
        }

      });
    }
    EasyMock.expect(mockServer.requestConquerableStations()).andReturn(stations);
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
    SyncStatus syncOutcome = ConquerableStationsSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<ConquerableStation> storedStations = retrieveAll(new BatchRetriever<ConquerableStation>() {

      @Override
      public List<ConquerableStation> getNextBatch(
                                                   List<ConquerableStation> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return ConquerableStation.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                              ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(conqStationTestData.length, storedStations.size());
    for (int i = 0; i < conqStationTestData.length; i++) {
      ConquerableStation nextStation = storedStations.get(i);
      Assert.assertEquals((long) (Long) conqStationTestData[i][0], nextStation.getCorporationID());
      Assert.assertEquals(conqStationTestData[i][1], nextStation.getCorporationName());
      Assert.assertEquals((long) (Long) conqStationTestData[i][2], nextStation.getSolarSystemID());
      Assert.assertEquals((long) (Long) conqStationTestData[i][3], nextStation.getStationID());
      Assert.assertEquals(conqStationTestData[i][4], nextStation.getStationName());
      Assert.assertEquals((int) (Integer) conqStationTestData[i][5], nextStation.getStationTypeID());
      Assert.assertEquals((long) (Long) conqStationTestData[i][6], nextStation.getX());
      Assert.assertEquals((long) (Long) conqStationTestData[i][7], nextStation.getY());
      Assert.assertEquals((long) (Long) conqStationTestData[i][8], nextStation.getZ());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getConquerableStationsExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getConquerableStationsStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getConquerableStationsDetail());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < conqStationTestData.length; i++) {
      // Make half the existing data have unseen station IDs. These items should be removed during the sync
      // since they will not be returned from the API.
      ConquerableStation newStation = new ConquerableStation(
          (Long) conqStationTestData[i][0] + 1, conqStationTestData[i][1] + "1", (Long) conqStationTestData[i][2] + 1,
          i % 2 == 0 ? TestBase.getUniqueRandomLong() : (long) (Long) conqStationTestData[i][3], conqStationTestData[i][4] + "1",
          (Integer) conqStationTestData[i][5] + 1, (Long) conqStationTestData[i][6] + 1, (Long) conqStationTestData[i][7] + 1,
          (Long) conqStationTestData[i][8] + 1);
      newStation.setup(testTime - 1);
      RefCachedData.updateData(newStation);
    }

    // Perform the sync
    SyncStatus syncOutcome = ConquerableStationsSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<ConquerableStation> storedStations = retrieveAll(new BatchRetriever<ConquerableStation>() {

      @Override
      public List<ConquerableStation> getNextBatch(
                                                   List<ConquerableStation> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return ConquerableStation.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                              ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(conqStationTestData.length, storedStations.size());
    for (int i = 0; i < conqStationTestData.length; i++) {
      ConquerableStation nextStation = storedStations.get(i);
      Assert.assertEquals((long) (Long) conqStationTestData[i][0], nextStation.getCorporationID());
      Assert.assertEquals(conqStationTestData[i][1], nextStation.getCorporationName());
      Assert.assertEquals((long) (Long) conqStationTestData[i][2], nextStation.getSolarSystemID());
      Assert.assertEquals((long) (Long) conqStationTestData[i][3], nextStation.getStationID());
      Assert.assertEquals(conqStationTestData[i][4], nextStation.getStationName());
      Assert.assertEquals((int) (Integer) conqStationTestData[i][5], nextStation.getStationTypeID());
      Assert.assertEquals((long) (Long) conqStationTestData[i][6], nextStation.getX());
      Assert.assertEquals((long) (Long) conqStationTestData[i][7], nextStation.getY());
      Assert.assertEquals((long) (Long) conqStationTestData[i][8], nextStation.getZ());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getConquerableStationsExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getConquerableStationsStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getConquerableStationsDetail());
  }

  // Test skips update when already updated
  @Test
  public void testSyncUpdateSkip() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < conqStationTestData.length; i++) {
      // Make half the existing data have unseen station IDs. These items should be removed during the sync
      // since they will not be returned from the API.
      ConquerableStation newStation = new ConquerableStation(
          (Long) conqStationTestData[i][0] + 1, conqStationTestData[i][1] + "1", (Long) conqStationTestData[i][2] + 1, (Long) conqStationTestData[i][3] + 1,
          conqStationTestData[i][4] + "1", (Integer) conqStationTestData[i][5] + 1, (Long) conqStationTestData[i][6] + 1, (Long) conqStationTestData[i][7] + 1,
          (Long) conqStationTestData[i][8] + 1);
      newStation.setup(testTime - 1);
      RefCachedData.updateData(newStation);
    }

    // Set the tracker as already updated and populate the container
    tracker.setConquerableStationsStatus(SyncState.UPDATED);
    tracker.setConquerableStationsDetail(null);
    tracker = RefSyncTracker.updateTracker(tracker);
    container.setConquerableStationsExpiry(prevDate);
    container = RefCachedData.updateData(container);

    // Perform the sync
    SyncStatus syncOutcome = ConquerableStationsSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    // Skip the verify here since the calls should never be made

    // Verify unchanged
    List<ConquerableStation> storedStations = retrieveAll(new BatchRetriever<ConquerableStation>() {

      @Override
      public List<ConquerableStation> getNextBatch(
                                                   List<ConquerableStation> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return ConquerableStation.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                              ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(conqStationTestData.length, storedStations.size());
    for (int i = 0; i < conqStationTestData.length; i++) {
      ConquerableStation nextStation = storedStations.get(i);
      Assert.assertEquals((Long) conqStationTestData[i][0] + 1, nextStation.getCorporationID());
      Assert.assertEquals(conqStationTestData[i][1] + "1", nextStation.getCorporationName());
      Assert.assertEquals((Long) conqStationTestData[i][2] + 1, nextStation.getSolarSystemID());
      Assert.assertEquals((Long) conqStationTestData[i][3] + 1, nextStation.getStationID());
      Assert.assertEquals(conqStationTestData[i][4] + "1", nextStation.getStationName());
      Assert.assertEquals((Integer) conqStationTestData[i][5] + 1, nextStation.getStationTypeID());
      Assert.assertEquals((Long) conqStationTestData[i][6] + 1, nextStation.getX());
      Assert.assertEquals((Long) conqStationTestData[i][7] + 1, nextStation.getY());
      Assert.assertEquals((Long) conqStationTestData[i][8] + 1, nextStation.getZ());
    }

    // Verify tracker and container unchanged
    Assert.assertEquals(prevDate, RefData.getRefData().getConquerableStationsExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getConquerableStationsStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getConquerableStationsDetail());
  }

}
