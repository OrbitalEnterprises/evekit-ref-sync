package enterprises.orbital.evekit.model.eve.sync;

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
import enterprises.orbital.evekit.model.eve.FactionStats;
import enterprises.orbital.evekit.model.eve.FactionWar;
import enterprises.orbital.evekit.model.eve.FactionWarSummary;
import enterprises.orbital.evexmlapi.eve.IEveAPI;
import enterprises.orbital.evexmlapi.eve.IFacWarSummary;
import enterprises.orbital.evexmlapi.eve.IFactionStats;
import enterprises.orbital.evexmlapi.eve.IFactionWar;

public class FacWarStatsSyncTest extends RefTestBase {

  // Local mocks and other objects
  long                testDate;
  long                prevDate;
  RefData             container;
  RefSyncTracker      tracker;
  RefSynchronizerUtil syncUtil;
  IEveAPI             mockServer;

  static Object[][]   facWarSummaryTestData;
  static Object[][]   facStatsTestData;
  static Object[][]   facWarTestData;

  static {
    // FactionWarSummary test data
    // 0 int killsLastWeek
    // 1 int killsTotal
    // 2 int killsYesterday
    // 3 int victoryPointsLastWeek
    // 4 int victoryPointsTotal
    // 5 int victoryPointsYesterday
    int size = 1;
    facWarSummaryTestData = new Object[size][6];
    for (int i = 0; i < size; i++) {
      facWarSummaryTestData[i][0] = TestBase.getRandomInt();
      facWarSummaryTestData[i][1] = TestBase.getRandomInt();
      facWarSummaryTestData[i][2] = TestBase.getRandomInt();
      facWarSummaryTestData[i][3] = TestBase.getRandomInt();
      facWarSummaryTestData[i][4] = TestBase.getRandomInt();
      facWarSummaryTestData[i][5] = TestBase.getRandomInt();
    }
    // FactionStats test data
    // 0 long factionID
    // 1 String factionName
    // 2 int killsLastWeek
    // 3 int killsTotal
    // 4 int killsYesterday
    // 5 int pilots
    // 6 int systemsControlled
    // 7 int victoryPointsLastWeek
    // 8 int victoryPointsTotal
    // 9 int victoryPointsYesterday
    size = 100 + TestBase.getRandomInt(100);
    facStatsTestData = new Object[size][10];
    for (int i = 0; i < size; i++) {
      facStatsTestData[i][0] = TestBase.getUniqueRandomLong();
      facStatsTestData[i][1] = TestBase.getRandomText(50);
      facStatsTestData[i][2] = TestBase.getRandomInt();
      facStatsTestData[i][3] = TestBase.getRandomInt();
      facStatsTestData[i][4] = TestBase.getRandomInt();
      facStatsTestData[i][5] = TestBase.getRandomInt();
      facStatsTestData[i][6] = TestBase.getRandomInt();
      facStatsTestData[i][7] = TestBase.getRandomInt();
      facStatsTestData[i][8] = TestBase.getRandomInt();
      facStatsTestData[i][9] = TestBase.getRandomInt();
    }
    // FactionWar test data
    // 0 long againstID
    // 1 String againstName
    // 2 long factionID
    // 3 String factionName
    size = 100 + TestBase.getRandomInt(100);
    facWarTestData = new Object[size][4];
    for (int i = 0; i < size; i++) {
      facWarTestData[i][0] = TestBase.getUniqueRandomLong();
      facWarTestData[i][1] = TestBase.getRandomText(50);
      facWarTestData[i][2] = TestBase.getUniqueRandomLong();
      facWarTestData[i][3] = TestBase.getRandomText(50);
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
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM FactionWarSummary").executeUpdate();
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM FactionStats").executeUpdate();
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM FactionWar").executeUpdate();
      }
    });

    super.teardown();
  }

  // Mock up server interface
  public void setupOkMock() throws Exception {
    mockServer = EasyMock.createMock(IEveAPI.class);
    final List<IFactionStats> facStats = new ArrayList<>();
    final List<IFactionWar> facWars = new ArrayList<>();
    for (int i = 0; i < facStatsTestData.length; i++) {
      final Object[] data = facStatsTestData[i];
      facStats.add(new IFactionStats() {

        @Override
        public long getFactionID() {
          return (Long) data[0];
        }

        @Override
        public String getFactionName() {
          return (String) data[1];
        }

        @Override
        public int getKillsLastWeek() {
          return (Integer) data[2];
        }

        @Override
        public int getKillsTotal() {
          return (Integer) data[3];
        }

        @Override
        public int getKillsYesterday() {
          return (Integer) data[4];
        }

        @Override
        public int getPilots() {
          return (Integer) data[5];
        }

        @Override
        public int getSystemsControlled() {
          return (Integer) data[6];
        }

        @Override
        public int getVictoryPointsLastWeek() {
          return (Integer) data[7];
        }

        @Override
        public int getVictoryPointsTotal() {
          return (Integer) data[8];
        }

        @Override
        public int getVictoryPointsYesterday() {
          return (Integer) data[9];
        }

      });
    }
    for (int i = 0; i < facWarTestData.length; i++) {
      final Object[] data = facWarTestData[i];
      facWars.add(new IFactionWar() {

        @Override
        public long getAgainstID() {
          return (Long) data[0];
        }

        @Override
        public String getAgainstName() {
          return (String) data[1];
        }

        @Override
        public long getFactionID() {
          return (Long) data[2];
        }

        @Override
        public String getFactionName() {
          return (String) data[3];
        }

      });
    }
    final IFacWarSummary summary = new IFacWarSummary() {

      @Override
      public Collection<IFactionStats> getFactions() {
        return facStats;
      }

      @Override
      public int getKillsLastWeek() {
        return (Integer) facWarSummaryTestData[0][0];
      }

      @Override
      public int getKillsTotal() {
        return (Integer) facWarSummaryTestData[0][1];
      }

      @Override
      public int getKillsYesterday() {
        return (Integer) facWarSummaryTestData[0][2];
      }

      @Override
      public int getVictoryPointsLastWeek() {
        return (Integer) facWarSummaryTestData[0][3];
      }

      @Override
      public int getVictoryPointsTotal() {
        return (Integer) facWarSummaryTestData[0][4];
      }

      @Override
      public int getVictoryPointsYesterday() {
        return (Integer) facWarSummaryTestData[0][5];
      }

      @Override
      public Collection<IFactionWar> getWars() {
        return facWars;
      }

    };
    EasyMock.expect(mockServer.requestFacWarStats()).andReturn(summary);
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
    SyncStatus syncOutcome = FacWarStatsSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<FactionWarSummary> storedSummary = retrieveAll(new BatchRetriever<FactionWarSummary>() {

      @Override
      public List<FactionWarSummary> getNextBatch(
                                                  List<FactionWarSummary> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return FactionWarSummary.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<FactionStats> storedStats = retrieveAll(new BatchRetriever<FactionStats>() {

      @Override
      public List<FactionStats> getNextBatch(
                                             List<FactionStats> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return FactionStats.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                        ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<FactionWar> storedWars = retrieveAll(new BatchRetriever<FactionWar>() {

      @Override
      public List<FactionWar> getNextBatch(
                                           List<FactionWar> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return FactionWar.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(facWarSummaryTestData.length, storedSummary.size());
    Assert.assertEquals(facStatsTestData.length, storedStats.size());
    Assert.assertEquals(facWarTestData.length, storedWars.size());
    for (int i = 0; i < facWarSummaryTestData.length; i++) {
      FactionWarSummary nextSummary = storedSummary.get(i);
      Assert.assertEquals((int) (Integer) facWarSummaryTestData[i][0], nextSummary.getKillsLastWeek());
      Assert.assertEquals((int) (Integer) facWarSummaryTestData[i][1], nextSummary.getKillsTotal());
      Assert.assertEquals((int) (Integer) facWarSummaryTestData[i][2], nextSummary.getKillsYesterday());
      Assert.assertEquals((int) (Integer) facWarSummaryTestData[i][3], nextSummary.getVictoryPointsLastWeek());
      Assert.assertEquals((int) (Integer) facWarSummaryTestData[i][4], nextSummary.getVictoryPointsTotal());
      Assert.assertEquals((int) (Integer) facWarSummaryTestData[i][5], nextSummary.getVictoryPointsYesterday());
    }
    for (int i = 0; i < facStatsTestData.length; i++) {
      FactionStats nextStats = storedStats.get(i);
      Assert.assertEquals((long) (Long) facStatsTestData[i][0], nextStats.getFactionID());
      Assert.assertEquals(facStatsTestData[i][1], nextStats.getFactionName());
      Assert.assertEquals((int) (Integer) facStatsTestData[i][2], nextStats.getKillsLastWeek());
      Assert.assertEquals((int) (Integer) facStatsTestData[i][3], nextStats.getKillsTotal());
      Assert.assertEquals((int) (Integer) facStatsTestData[i][4], nextStats.getKillsYesterday());
      Assert.assertEquals((int) (Integer) facStatsTestData[i][5], nextStats.getPilots());
      Assert.assertEquals((int) (Integer) facStatsTestData[i][6], nextStats.getSystemsControlled());
      Assert.assertEquals((int) (Integer) facStatsTestData[i][7], nextStats.getVictoryPointsLastWeek());
      Assert.assertEquals((int) (Integer) facStatsTestData[i][8], nextStats.getVictoryPointsTotal());
      Assert.assertEquals((int) (Integer) facStatsTestData[i][9], nextStats.getVictoryPointsYesterday());
    }
    for (int i = 0; i < facWarTestData.length; i++) {
      FactionWar nextWar = storedWars.get(i);
      Assert.assertEquals((long) (Long) facWarTestData[i][0], nextWar.getAgainstID());
      Assert.assertEquals(facWarTestData[i][1], nextWar.getAgainstName());
      Assert.assertEquals((long) (Long) facWarTestData[i][2], nextWar.getFactionID());
      Assert.assertEquals(facWarTestData[i][3], nextWar.getFactionName());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getFacWarStatsExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getFacWarStatsStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getFacWarStatsDetail());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < facWarSummaryTestData.length; i++) {
      FactionWarSummary newSummary = new FactionWarSummary(
          (Integer) facWarSummaryTestData[i][0] + 1, (Integer) facWarSummaryTestData[i][1] + 1, (Integer) facWarSummaryTestData[i][2] + 1,
          (Integer) facWarSummaryTestData[i][3] + 1, (Integer) facWarSummaryTestData[i][4] + 1, (Integer) facWarSummaryTestData[i][5] + 1);
      newSummary.setup(testTime - 1);
      RefCachedData.updateData(newSummary);
    }
    for (int i = 0; i < facStatsTestData.length; i++) {
      // Make half the existing data have unseen faction IDs. These items should be removed during the sync
      // since they will not be returned from the API.
      FactionStats newStats = new FactionStats(
          i % 2 == 0 ? TestBase.getUniqueRandomLong() : (Long) facStatsTestData[i][0], (String) facStatsTestData[i][1] + "1",
          (Integer) facStatsTestData[i][2] + 1, (Integer) facStatsTestData[i][3] + 1, (Integer) facStatsTestData[i][4] + 1,
          (Integer) facStatsTestData[i][5] + 1, (Integer) facStatsTestData[i][6] + 1, (Integer) facStatsTestData[i][7] + 1,
          (Integer) facStatsTestData[i][8] + 1, (Integer) facStatsTestData[i][9] + 1);
      newStats.setup(testTime - 1);
      RefCachedData.updateData(newStats);
    }
    for (int i = 0; i < facWarTestData.length; i++) {
      // Make half the existing data have unseen faction IDs. These items should be removed during the sync
      // since they will not be returned from the API.
      FactionWar newWar = new FactionWar(
          i % 2 == 0 ? TestBase.getUniqueRandomLong() : (Long) facWarTestData[i][0], (String) facWarTestData[i][1] + "1",
          i % 2 == 0 ? TestBase.getUniqueRandomLong() : (Long) facWarTestData[i][2], (String) facWarTestData[i][3] + "1");
      newWar.setup(testTime - 1);
      RefCachedData.updateData(newWar);
    }

    // Perform the sync
    SyncStatus syncOutcome = FacWarStatsSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<FactionWarSummary> storedSummary = retrieveAll(new BatchRetriever<FactionWarSummary>() {

      @Override
      public List<FactionWarSummary> getNextBatch(
                                                  List<FactionWarSummary> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return FactionWarSummary.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<FactionStats> storedStats = retrieveAll(new BatchRetriever<FactionStats>() {

      @Override
      public List<FactionStats> getNextBatch(
                                             List<FactionStats> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return FactionStats.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                        ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<FactionWar> storedWars = retrieveAll(new BatchRetriever<FactionWar>() {

      @Override
      public List<FactionWar> getNextBatch(
                                           List<FactionWar> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return FactionWar.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(facWarSummaryTestData.length, storedSummary.size());
    Assert.assertEquals(facStatsTestData.length, storedStats.size());
    Assert.assertEquals(facWarTestData.length, storedWars.size());
    for (int i = 0; i < facWarSummaryTestData.length; i++) {
      FactionWarSummary nextSummary = storedSummary.get(i);
      Assert.assertEquals((int) (Integer) facWarSummaryTestData[i][0], nextSummary.getKillsLastWeek());
      Assert.assertEquals((int) (Integer) facWarSummaryTestData[i][1], nextSummary.getKillsTotal());
      Assert.assertEquals((int) (Integer) facWarSummaryTestData[i][2], nextSummary.getKillsYesterday());
      Assert.assertEquals((int) (Integer) facWarSummaryTestData[i][3], nextSummary.getVictoryPointsLastWeek());
      Assert.assertEquals((int) (Integer) facWarSummaryTestData[i][4], nextSummary.getVictoryPointsTotal());
      Assert.assertEquals((int) (Integer) facWarSummaryTestData[i][5], nextSummary.getVictoryPointsYesterday());
    }
    for (int i = 0; i < facStatsTestData.length; i++) {
      FactionStats nextStats = storedStats.get(i);
      Assert.assertEquals((long) (Long) facStatsTestData[i][0], nextStats.getFactionID());
      Assert.assertEquals(facStatsTestData[i][1], nextStats.getFactionName());
      Assert.assertEquals((int) (Integer) facStatsTestData[i][2], nextStats.getKillsLastWeek());
      Assert.assertEquals((int) (Integer) facStatsTestData[i][3], nextStats.getKillsTotal());
      Assert.assertEquals((int) (Integer) facStatsTestData[i][4], nextStats.getKillsYesterday());
      Assert.assertEquals((int) (Integer) facStatsTestData[i][5], nextStats.getPilots());
      Assert.assertEquals((int) (Integer) facStatsTestData[i][6], nextStats.getSystemsControlled());
      Assert.assertEquals((int) (Integer) facStatsTestData[i][7], nextStats.getVictoryPointsLastWeek());
      Assert.assertEquals((int) (Integer) facStatsTestData[i][8], nextStats.getVictoryPointsTotal());
      Assert.assertEquals((int) (Integer) facStatsTestData[i][9], nextStats.getVictoryPointsYesterday());
    }
    for (int i = 0; i < facWarTestData.length; i++) {
      FactionWar nextWar = storedWars.get(i);
      Assert.assertEquals((long) (Long) facWarTestData[i][0], nextWar.getAgainstID());
      Assert.assertEquals(facWarTestData[i][1], nextWar.getAgainstName());
      Assert.assertEquals((long) (Long) facWarTestData[i][2], nextWar.getFactionID());
      Assert.assertEquals(facWarTestData[i][3], nextWar.getFactionName());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getFacWarStatsExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getFacWarStatsStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getFacWarStatsDetail());
  }

  // Test skips update when already updated
  @Test
  public void testSyncUpdateSkip() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < facWarSummaryTestData.length; i++) {
      FactionWarSummary newSummary = new FactionWarSummary(
          (Integer) facWarSummaryTestData[i][0] + 1, (Integer) facWarSummaryTestData[i][1] + 1, (Integer) facWarSummaryTestData[i][2] + 1,
          (Integer) facWarSummaryTestData[i][3] + 1, (Integer) facWarSummaryTestData[i][4] + 1, (Integer) facWarSummaryTestData[i][5] + 1);
      newSummary.setup(testTime - 1);
      RefCachedData.updateData(newSummary);
    }
    for (int i = 0; i < facStatsTestData.length; i++) {
      FactionStats newStats = new FactionStats(
          (Long) facStatsTestData[i][0] + 1, (String) facStatsTestData[i][1] + "1", (Integer) facStatsTestData[i][2] + 1, (Integer) facStatsTestData[i][3] + 1,
          (Integer) facStatsTestData[i][4] + 1, (Integer) facStatsTestData[i][5] + 1, (Integer) facStatsTestData[i][6] + 1,
          (Integer) facStatsTestData[i][7] + 1, (Integer) facStatsTestData[i][8] + 1, (Integer) facStatsTestData[i][9] + 1);
      newStats.setup(testTime - 1);
      RefCachedData.updateData(newStats);
    }
    for (int i = 0; i < facWarTestData.length; i++) {
      FactionWar newWar = new FactionWar(
          (Long) facWarTestData[i][0] + 1, (String) facWarTestData[i][1] + "1", (Long) facWarTestData[i][2] + 1, (String) facWarTestData[i][3] + "1");
      newWar.setup(testTime - 1);
      RefCachedData.updateData(newWar);
    }

    // Set the tracker as already updated and populate the container
    tracker.setFacWarStatsStatus(SyncState.UPDATED);
    tracker.setFacWarStatsDetail(null);
    tracker = RefSyncTracker.updateTracker(tracker);
    container.setFacWarStatsExpiry(prevDate);
    container = RefCachedData.updateData(container);

    // Perform the sync
    SyncStatus syncOutcome = FacWarStatsSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    // Skip the verify here since the calls should never be made

    // Verify unchanged
    List<FactionWarSummary> storedSummary = retrieveAll(new BatchRetriever<FactionWarSummary>() {

      @Override
      public List<FactionWarSummary> getNextBatch(
                                                  List<FactionWarSummary> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return FactionWarSummary.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<FactionStats> storedStats = retrieveAll(new BatchRetriever<FactionStats>() {

      @Override
      public List<FactionStats> getNextBatch(
                                             List<FactionStats> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return FactionStats.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                        ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<FactionWar> storedWars = retrieveAll(new BatchRetriever<FactionWar>() {

      @Override
      public List<FactionWar> getNextBatch(
                                           List<FactionWar> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return FactionWar.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(facWarSummaryTestData.length, storedSummary.size());
    Assert.assertEquals(facStatsTestData.length, storedStats.size());
    Assert.assertEquals(facWarTestData.length, storedWars.size());
    for (int i = 0; i < facWarSummaryTestData.length; i++) {
      FactionWarSummary nextSummary = storedSummary.get(i);
      Assert.assertEquals((Integer) facWarSummaryTestData[i][0] + 1, nextSummary.getKillsLastWeek());
      Assert.assertEquals((Integer) facWarSummaryTestData[i][1] + 1, nextSummary.getKillsTotal());
      Assert.assertEquals((Integer) facWarSummaryTestData[i][2] + 1, nextSummary.getKillsYesterday());
      Assert.assertEquals((Integer) facWarSummaryTestData[i][3] + 1, nextSummary.getVictoryPointsLastWeek());
      Assert.assertEquals((Integer) facWarSummaryTestData[i][4] + 1, nextSummary.getVictoryPointsTotal());
      Assert.assertEquals((Integer) facWarSummaryTestData[i][5] + 1, nextSummary.getVictoryPointsYesterday());
    }
    for (int i = 0; i < facStatsTestData.length; i++) {
      FactionStats nextStats = storedStats.get(i);
      Assert.assertEquals((Long) facStatsTestData[i][0] + 1, nextStats.getFactionID());
      Assert.assertEquals(facStatsTestData[i][1] + "1", nextStats.getFactionName());
      Assert.assertEquals((Integer) facStatsTestData[i][2] + 1, nextStats.getKillsLastWeek());
      Assert.assertEquals((Integer) facStatsTestData[i][3] + 1, nextStats.getKillsTotal());
      Assert.assertEquals((Integer) facStatsTestData[i][4] + 1, nextStats.getKillsYesterday());
      Assert.assertEquals((Integer) facStatsTestData[i][5] + 1, nextStats.getPilots());
      Assert.assertEquals((Integer) facStatsTestData[i][6] + 1, nextStats.getSystemsControlled());
      Assert.assertEquals((Integer) facStatsTestData[i][7] + 1, nextStats.getVictoryPointsLastWeek());
      Assert.assertEquals((Integer) facStatsTestData[i][8] + 1, nextStats.getVictoryPointsTotal());
      Assert.assertEquals((Integer) facStatsTestData[i][9] + 1, nextStats.getVictoryPointsYesterday());
    }
    for (int i = 0; i < facWarTestData.length; i++) {
      FactionWar nextWar = storedWars.get(i);
      Assert.assertEquals((Long) facWarTestData[i][0] + 1, nextWar.getAgainstID());
      Assert.assertEquals(facWarTestData[i][1] + "1", nextWar.getAgainstName());
      Assert.assertEquals((Long) facWarTestData[i][2] + 1, nextWar.getFactionID());
      Assert.assertEquals(facWarTestData[i][3] + "1", nextWar.getFactionName());
    }

    // Verify tracker and container unchanged
    Assert.assertEquals(prevDate, RefData.getRefData().getFacWarStatsExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getFacWarStatsStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getFacWarStatsDetail());
  }

}
