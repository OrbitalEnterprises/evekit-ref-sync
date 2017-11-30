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
import enterprises.orbital.evekit.model.eve.RefType;
import enterprises.orbital.evexmlapi.eve.IEveAPI;
import enterprises.orbital.evexmlapi.eve.IRefType;

public class RefTypeSyncTest extends RefTestBase {

  // Local mocks and other objects
  long                testDate;
  long                prevDate;
  RefData             container;
  RefSyncTracker      tracker;
  RefSynchronizerUtil syncUtil;
  IEveAPI             mockServer;

  static Object[][]   refTypeTestData;

  static {
    // RefType test data
    // 0 int refTypeID
    // 1 String refTypeName
    int size = 20 + TestBase.getRandomInt(20);
    refTypeTestData = new Object[size][2];
    for (int i = 0; i < size; i++) {
      refTypeTestData[i][0] = TestBase.getUniqueRandomInteger();
      refTypeTestData[i][1] = TestBase.getRandomText(50);
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
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM RefType").executeUpdate();
      }
    });

    super.teardown();
  }

  // Mock up server interface
  public void setupOkMock() throws Exception {
    mockServer = EasyMock.createMock(IEveAPI.class);
    final List<IRefType> refTypes = new ArrayList<>();
    for (int i = 0; i < refTypeTestData.length; i++) {
      final Object[] data = refTypeTestData[i];
      refTypes.add(new IRefType() {

        @Override
        public int getRefTypeID() {
          return (Integer) data[0];
        }

        @Override
        public String getRefTypeName() {
          return (String) data[1];
        }

      });
    }
    EasyMock.expect(mockServer.requestRefTypes()).andReturn(refTypes);
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
    SyncStatus syncOutcome = RefTypeSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<RefType> storedTypes = retrieveAll(new BatchRetriever<RefType>() {

      @Override
      public List<RefType> getNextBatch(
                                        List<RefType> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return RefType.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(refTypeTestData.length, storedTypes.size());
    for (int i = 0; i < refTypeTestData.length; i++) {
      RefType nextType = storedTypes.get(i);
      Assert.assertEquals((int) (Integer) refTypeTestData[i][0], nextType.getRefTypeID());
      Assert.assertEquals(refTypeTestData[i][1], nextType.getRefTypeName());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getRefTypeExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getRefTypeStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getRefTypeDetail());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < refTypeTestData.length; i++) {
      // Make half the existing data have unseen ref type IDs. These items should be removed during the sync
      // since they will not be returned from the API.
      RefType newType = new RefType(i % 2 == 0 ? TestBase.getUniqueRandomInteger() : (int) (Integer) refTypeTestData[i][0], refTypeTestData[i][1] + "1");
      newType.setup(testTime - 1);
      RefCachedData.updateData(newType);
    }

    // Perform the sync
    SyncStatus syncOutcome = RefTypeSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<RefType> storedTypes = retrieveAll(new BatchRetriever<RefType>() {

      @Override
      public List<RefType> getNextBatch(
                                        List<RefType> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return RefType.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(refTypeTestData.length, storedTypes.size());
    for (int i = 0; i < refTypeTestData.length; i++) {
      RefType nextType = storedTypes.get(i);
      Assert.assertEquals((int) (Integer) refTypeTestData[i][0], nextType.getRefTypeID());
      Assert.assertEquals(refTypeTestData[i][1], nextType.getRefTypeName());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getRefTypeExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getRefTypeStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getRefTypeDetail());
  }

  // Test skips update when already updated
  @Test
  public void testSyncUpdateSkip() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < refTypeTestData.length; i++) {
      RefType newType = new RefType((Integer) refTypeTestData[i][0] + 1, refTypeTestData[i][1] + "1");
      newType.setup(testTime - 1);
      RefCachedData.updateData(newType);
    }

    // Set the tracker as already updated and populate the container
    tracker.setRefTypeStatus(SyncState.UPDATED);
    tracker.setRefTypeDetail(null);
    tracker = RefSyncTracker.updateTracker(tracker);
    container.setRefTypeExpiry(prevDate);
    container = RefCachedData.updateData(container);

    // Perform the sync
    SyncStatus syncOutcome = RefTypeSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    // Skip the verify here since the calls should never be made

    // Verify unchanged
    List<RefType> storedTypes = retrieveAll(new BatchRetriever<RefType>() {

      @Override
      public List<RefType> getNextBatch(
                                        List<RefType> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return RefType.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(refTypeTestData.length, storedTypes.size());
    for (int i = 0; i < refTypeTestData.length; i++) {
      RefType nextType = storedTypes.get(i);
      Assert.assertEquals((Integer) refTypeTestData[i][0] + 1, nextType.getRefTypeID());
      Assert.assertEquals(refTypeTestData[i][1] + "1", nextType.getRefTypeName());
    }

    // Verify tracker and container unchanged
    Assert.assertEquals(prevDate, RefData.getRefData().getRefTypeExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getRefTypeStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getRefTypeDetail());
  }

}
