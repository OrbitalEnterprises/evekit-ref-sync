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
import enterprises.orbital.evekit.model.eve.Alliance;
import enterprises.orbital.evekit.model.eve.AllianceMemberCorporation;
import enterprises.orbital.evexmlapi.eve.IAlliance;
import enterprises.orbital.evexmlapi.eve.IEveAPI;
import enterprises.orbital.evexmlapi.eve.IMemberCorporation;

public class AllianceSyncTest extends RefTestBase {

  // Local mocks and other objects
  long                testDate;
  long                prevDate;
  RefData             container;
  RefSyncTracker      tracker;
  RefSynchronizerUtil syncUtil;
  IEveAPI             mockServer;

  static Object[][]   allianceTestData;
  static Object[][]   allianceMemberTestData;

  static {
    // Alliance test data
    // 0 long allianceID;
    // 1 long executorCorpID;
    // 2 int memberCount;
    // 3 String name;
    // 4 String shortName;
    // 5 long startDate;
    int size = 20 + TestBase.getRandomInt(20);
    allianceTestData = new Object[size][6];
    for (int i = 0; i < size; i++) {
      allianceTestData[i][0] = TestBase.getUniqueRandomLong();
      allianceTestData[i][1] = TestBase.getRandomLong();
      allianceTestData[i][2] = TestBase.getRandomInt();
      allianceTestData[i][3] = TestBase.getRandomText(50);
      allianceTestData[i][4] = TestBase.getRandomText(50);
      allianceTestData[i][5] = TestBase.getRandomLong();
    }
    // Alliance member corp test data
    // 0 long allianceID;
    // 1 long corporationID;
    // 2 long startDate;
    size = 100 + TestBase.getRandomInt(100);
    allianceMemberTestData = new Object[size][3];
    for (int i = 0, j = 0; i < size; i++, j = (j + 1) % allianceTestData.length) {
      allianceMemberTestData[i][0] = allianceTestData[j][0];
      allianceMemberTestData[i][1] = TestBase.getRandomLong();
      allianceMemberTestData[i][2] = TestBase.getRandomLong();
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
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM Alliance").executeUpdate();
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM AllianceMemberCorporation").executeUpdate();
      }
    });

    super.teardown();
  }

  // Mock up server interface
  public void setupOkMock() throws Exception {
    mockServer = EasyMock.createMock(IEveAPI.class);
    final List<IAlliance> alliances = new ArrayList<>();
    final List<IMemberCorporation> allianceMembers = new ArrayList<>();
    for (int i = 0; i < allianceMemberTestData.length; i++) {
      final Object[] data = allianceMemberTestData[i];
      allianceMembers.add(new IMemberCorporation() {

        @Override
        public long getCorporationID() {
          return (Long) data[1];
        }

        @Override
        public Date getStartDate() {
          return new Date((Long) data[2]);
        }

      });
    }
    for (int i = 0; i < allianceTestData.length; i++) {
      final Object[] data = allianceTestData[i];
      alliances.add(new IAlliance() {

        @Override
        public long getAllianceID() {
          return (Long) data[0];
        }

        @Override
        public long getExecutorCorpID() {
          return (Long) data[1];
        }

        @Override
        public Collection<IMemberCorporation> getMemberCorporations() {
          List<IMemberCorporation> memList = new ArrayList<>();
          for (int i = 0; i < allianceMemberTestData.length; i++) {
            if ((Long) allianceMemberTestData[i][0] == (Long) data[0]) memList.add(allianceMembers.get(i));
          }
          return memList;
        }

        @Override
        public int getMemberCount() {
          return (Integer) data[2];
        }

        @Override
        public String getName() {
          return (String) data[3];
        }

        @Override
        public String getShortName() {
          return (String) data[4];
        }

        @Override
        public Date getStartDate() {
          return new Date((Long) data[5]);
        }

      });
    }
    EasyMock.expect(mockServer.requestAlliances()).andReturn(alliances);
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
    SyncStatus syncOutcome = AllianceSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<Alliance> storedAlliances = retrieveAll(new BatchRetriever<Alliance>() {

      @Override
      public List<Alliance> getNextBatch(
                                         List<Alliance> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return Alliance.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<AllianceMemberCorporation> storedMembers = retrieveAll(new BatchRetriever<AllianceMemberCorporation>() {

      @Override
      public List<AllianceMemberCorporation> getNextBatch(
                                                          List<AllianceMemberCorporation> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return AllianceMemberCorporation.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(allianceTestData.length, storedAlliances.size());
    Assert.assertEquals(allianceMemberTestData.length, storedMembers.size());
    for (int i = 0; i < allianceTestData.length; i++) {
      Alliance nextAlliance = storedAlliances.get(i);
      Assert.assertEquals((long) (Long) allianceTestData[i][0], nextAlliance.getAllianceID());
      Assert.assertEquals((long) (Long) allianceTestData[i][1], nextAlliance.getExecutorCorpID());
      Assert.assertEquals((int) (Integer) allianceTestData[i][2], nextAlliance.getMemberCount());
      Assert.assertEquals(allianceTestData[i][3], nextAlliance.getName());
      Assert.assertEquals(allianceTestData[i][4], nextAlliance.getShortName());
      Assert.assertEquals((long) (Long) allianceTestData[i][5], nextAlliance.getStartDate());
    }
    // Alliance member list will be sorted in the order the alliances appeared in the test data.
    // This means we have to sort test comparison data in the order the alliances appear.
    Object[][] sortedAllianceMemberTestData = new Object[allianceMemberTestData.length][3];
    for (int i = 0, k = 0; i < allianceTestData.length; i++) {
      long allianceID = (Long) allianceTestData[i][0];
      for (int j = 0; j < allianceMemberTestData.length; j++) {
        if (allianceID == (Long) allianceMemberTestData[j][0]) {
          sortedAllianceMemberTestData[k][0] = allianceMemberTestData[j][0];
          sortedAllianceMemberTestData[k][1] = allianceMemberTestData[j][1];
          sortedAllianceMemberTestData[k][2] = allianceMemberTestData[j][2];
          k++;
        }
      }
    }
    for (int i = 0; i < sortedAllianceMemberTestData.length; i++) {
      AllianceMemberCorporation nextMember = storedMembers.get(i);
      Assert.assertEquals((long) (Long) sortedAllianceMemberTestData[i][0], nextMember.getAllianceID());
      Assert.assertEquals((long) (Long) sortedAllianceMemberTestData[i][1], nextMember.getCorporationID());
      Assert.assertEquals((long) (Long) sortedAllianceMemberTestData[i][2], nextMember.getStartDate());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getAllianceListExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getAllianceListStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getAllianceListDetail());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < allianceTestData.length; i++) {
      // Make half the existing data have unseen alliance IDs. These items should be removed during the sync
      // since they will not be returned from the API.
      Alliance newAlliance = new Alliance(
          i % 2 == 0 ? TestBase.getUniqueRandomLong() : (long) (Long) allianceTestData[i][0], (Long) allianceTestData[i][1] + 1,
          (Integer) allianceTestData[i][2] + 1, allianceTestData[i][3] + "1", allianceTestData[i][4] + "1", (Long) allianceTestData[i][5] + 1);
      newAlliance.setup(testTime - 1);
      RefCachedData.updateData(newAlliance);
    }
    for (int i = 0; i < allianceMemberTestData.length; i++) {
      // Make half the existing data have unseen corporation IDs. These items should be removed during the sync
      // since they will not be returned from the API.
      AllianceMemberCorporation newMember = new AllianceMemberCorporation(
          (Long) allianceMemberTestData[i][0], i % 2 == 0 ? TestBase.getUniqueRandomLong() : (long) (Long) allianceMemberTestData[i][1],
          (Long) allianceMemberTestData[i][2] + 1);
      newMember.setup(testTime - 1);
      RefCachedData.updateData(newMember);
    }

    // Perform the sync
    SyncStatus syncOutcome = AllianceSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<Alliance> storedAlliances = retrieveAll(new BatchRetriever<Alliance>() {

      @Override
      public List<Alliance> getNextBatch(
                                         List<Alliance> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return Alliance.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<AllianceMemberCorporation> storedMembers = retrieveAll(new BatchRetriever<AllianceMemberCorporation>() {

      @Override
      public List<AllianceMemberCorporation> getNextBatch(
                                                          List<AllianceMemberCorporation> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return AllianceMemberCorporation.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(allianceTestData.length, storedAlliances.size());
    Assert.assertEquals(allianceMemberTestData.length, storedMembers.size());
    for (int i = 0; i < allianceTestData.length; i++) {
      Alliance nextAlliance = storedAlliances.get(i);
      Assert.assertEquals((long) (Long) allianceTestData[i][0], nextAlliance.getAllianceID());
      Assert.assertEquals((long) (Long) allianceTestData[i][1], nextAlliance.getExecutorCorpID());
      Assert.assertEquals((int) (Integer) allianceTestData[i][2], nextAlliance.getMemberCount());
      Assert.assertEquals(allianceTestData[i][3], nextAlliance.getName());
      Assert.assertEquals(allianceTestData[i][4], nextAlliance.getShortName());
      Assert.assertEquals((long) (Long) allianceTestData[i][5], nextAlliance.getStartDate());
    }
    // Alliance member list will be sorted in the order the alliances appeared in the test data.
    // This means we have to sort test comparison data in the order the alliances appear.
    Object[][] sortedAllianceMemberTestData = new Object[allianceMemberTestData.length][3];
    for (int i = 0, k = 0; i < allianceTestData.length; i++) {
      long allianceID = (Long) allianceTestData[i][0];
      for (int j = 0; j < allianceMemberTestData.length; j++) {
        if (allianceID == (Long) allianceMemberTestData[j][0]) {
          sortedAllianceMemberTestData[k][0] = allianceMemberTestData[j][0];
          sortedAllianceMemberTestData[k][1] = allianceMemberTestData[j][1];
          sortedAllianceMemberTestData[k][2] = allianceMemberTestData[j][2];
          k++;
        }
      }
    }
    for (int i = 0; i < sortedAllianceMemberTestData.length; i++) {
      AllianceMemberCorporation nextMember = storedMembers.get(i);
      Assert.assertEquals((long) (Long) sortedAllianceMemberTestData[i][0], nextMember.getAllianceID());
      Assert.assertEquals((long) (Long) sortedAllianceMemberTestData[i][1], nextMember.getCorporationID());
      Assert.assertEquals((long) (Long) sortedAllianceMemberTestData[i][2], nextMember.getStartDate());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getAllianceListExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getAllianceListStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getAllianceListDetail());
  }

  // Test skips update when already updated
  @Test
  public void testSyncUpdateSkip() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < allianceTestData.length; i++) {
      // Make half the existing data have unseen alliance IDs. These items should be removed during the sync
      // since they will not be returned from the API.
      Alliance newAlliance = new Alliance(
          (Long) allianceTestData[i][0] + 1, (Long) allianceTestData[i][1] + 1, (Integer) allianceTestData[i][2] + 1, allianceTestData[i][3] + "1",
          allianceTestData[i][4] + "1", (Long) allianceTestData[i][5] + 1);
      newAlliance.setup(testTime - 1);
      RefCachedData.updateData(newAlliance);
    }
    for (int i = 0; i < allianceMemberTestData.length; i++) {
      // Make half the existing data have unseen corporation IDs. These items should be removed during the sync
      // since they will not be returned from the API.
      AllianceMemberCorporation newMember = new AllianceMemberCorporation(
          (Long) allianceMemberTestData[i][0] + 1, (Long) allianceMemberTestData[i][1] + 1, (Long) allianceMemberTestData[i][2] + 1);
      newMember.setup(testTime - 1);
      RefCachedData.updateData(newMember);
    }

    // Set the tracker as already updated and populate the container
    tracker.setAllianceListStatus(SyncState.UPDATED);
    tracker.setAllianceListDetail(null);
    tracker = RefSyncTracker.updateTracker(tracker);
    container.setAllianceListExpiry(prevDate);
    container = RefCachedData.updateData(container);

    // Perform the sync
    SyncStatus syncOutcome = AllianceSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    // Skip the verify here since the calls should never be made

    // Verify unchanged
    List<Alliance> storedAlliances = retrieveAll(new BatchRetriever<Alliance>() {

      @Override
      public List<Alliance> getNextBatch(
                                         List<Alliance> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return Alliance.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<AllianceMemberCorporation> storedMembers = retrieveAll(new BatchRetriever<AllianceMemberCorporation>() {

      @Override
      public List<AllianceMemberCorporation> getNextBatch(
                                                          List<AllianceMemberCorporation> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return AllianceMemberCorporation.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(allianceTestData.length, storedAlliances.size());
    Assert.assertEquals(allianceMemberTestData.length, storedMembers.size());
    for (int i = 0; i < allianceTestData.length; i++) {
      Alliance nextAlliance = storedAlliances.get(i);
      Assert.assertEquals((Long) allianceTestData[i][0] + 1, nextAlliance.getAllianceID());
      Assert.assertEquals((Long) allianceTestData[i][1] + 1, nextAlliance.getExecutorCorpID());
      Assert.assertEquals((Integer) allianceTestData[i][2] + 1, nextAlliance.getMemberCount());
      Assert.assertEquals(allianceTestData[i][3] + "1", nextAlliance.getName());
      Assert.assertEquals(allianceTestData[i][4] + "1", nextAlliance.getShortName());
      Assert.assertEquals((Long) allianceTestData[i][5] + 1, nextAlliance.getStartDate());
    }
    for (int i = 0; i < allianceMemberTestData.length; i++) {
      AllianceMemberCorporation nextMember = storedMembers.get(i);
      Assert.assertEquals((Long) allianceMemberTestData[i][0] + 1, nextMember.getAllianceID());
      Assert.assertEquals((Long) allianceMemberTestData[i][1] + 1, nextMember.getCorporationID());
      Assert.assertEquals((Long) allianceMemberTestData[i][2] + 1, nextMember.getStartDate());
    }

    // Verify tracker and container unchanged
    Assert.assertEquals(prevDate, RefData.getRefData().getAllianceListExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getAllianceListStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getAllianceListDetail());
  }

}
