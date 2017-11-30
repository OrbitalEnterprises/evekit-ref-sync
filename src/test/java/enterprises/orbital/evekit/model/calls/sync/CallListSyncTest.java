package enterprises.orbital.evekit.model.calls.sync;

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
import enterprises.orbital.evekit.model.calls.Call;
import enterprises.orbital.evekit.model.calls.CallGroup;
import enterprises.orbital.evexmlapi.api.IApiAPI;
import enterprises.orbital.evexmlapi.api.ICall;
import enterprises.orbital.evexmlapi.api.ICallGroup;
import enterprises.orbital.evexmlapi.api.ICallList;

public class CallListSyncTest extends RefTestBase {

  // Local mocks and other objects
  long                testDate;
  long                prevDate;
  RefData             container;
  RefSyncTracker      tracker;
  RefSynchronizerUtil syncUtil;
  IApiAPI             mockServer;

  static Object[][]   callGroupTestData;
  static Object[][]   callTestData;

  static {
    // Call group test data
    // 0 long groupID
    // 1 String name
    // 2 String description
    int size = 20 + TestBase.getRandomInt(20);
    callGroupTestData = new Object[size][3];
    for (int i = 0; i < size; i++) {
      callGroupTestData[i][0] = TestBase.getUniqueRandomLong();
      callGroupTestData[i][1] = TestBase.getRandomText(50);
      callGroupTestData[i][2] = TestBase.getRandomText(50);
    }
    // Call test data
    // 0 long accessMask
    // 1 String type
    // 2 String name
    // 3 long groupID
    // 4 String description
    size = 100 + TestBase.getRandomInt(100);
    callTestData = new Object[size][5];
    for (int i = 0; i < size; i++) {
      callTestData[i][0] = TestBase.getRandomLong();
      callTestData[i][1] = TestBase.getRandomText(50);
      callTestData[i][2] = TestBase.getRandomText(50);
      callTestData[i][3] = TestBase.getRandomLong();
      callTestData[i][4] = TestBase.getRandomText(50);
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
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM Call").executeUpdate();
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM CallGroup").executeUpdate();
      }
    });

    super.teardown();
  }

  // Mock up server interface
  public void setupOkMock() throws Exception {
    mockServer = EasyMock.createMock(IApiAPI.class);
    final List<ICallGroup> callGroups = new ArrayList<>();
    final List<ICall> calls = new ArrayList<>();
    for (int i = 0; i < callGroupTestData.length; i++) {
      final Object[] data = callGroupTestData[i];
      callGroups.add(new ICallGroup() {

        @Override
        public long getGroupID() {
          return (Long) data[0];
        }

        @Override
        public String getName() {
          return (String) data[1];
        }

        @Override
        public String getDescription() {
          return (String) data[2];
        }

      });
    }
    for (int i = 0; i < callTestData.length; i++) {
      final Object[] data = callTestData[i];
      calls.add(new ICall() {

        @Override
        public long getAccessMask() {
          return (Long) data[0];
        }

        @Override
        public String getType() {
          return (String) data[1];
        }

        @Override
        public String getName() {
          return (String) data[2];
        }

        @Override
        public long getGroupID() {
          return (Long) data[3];
        }

        @Override
        public String getDescription() {
          return (String) data[4];
        }

      });
    }
    ICallList mockCallList = new ICallList() {

      @Override
      public Collection<ICallGroup> getCallGroups() {
        return callGroups;
      }

      @Override
      public Collection<ICall> getCalls() {
        return calls;
      }

    };
    EasyMock.expect(mockServer.requestCallList()).andReturn(mockCallList);
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
    SyncStatus syncOutcome = CallListSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<CallGroup> storedGroups = retrieveAll(new BatchRetriever<CallGroup>() {

      @Override
      public List<CallGroup> getNextBatch(
                                          List<CallGroup> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return CallGroup.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<Call> storedCalls = retrieveAll(new BatchRetriever<Call>() {

      @Override
      public List<Call> getNextBatch(
                                     List<Call> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return Call.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(callGroupTestData.length, storedGroups.size());
    Assert.assertEquals(callTestData.length, storedCalls.size());
    for (int i = 0; i < callGroupTestData.length; i++) {
      CallGroup nextGroup = storedGroups.get(i);
      Assert.assertEquals((long) (Long) callGroupTestData[i][0], nextGroup.getGroupID());
      Assert.assertEquals(callGroupTestData[i][1], nextGroup.getName());
      Assert.assertEquals(callGroupTestData[i][2], nextGroup.getDescription());
    }
    for (int i = 0; i < callTestData.length; i++) {
      Call nextCall = storedCalls.get(i);
      Assert.assertEquals((long) (Long) callTestData[i][0], nextCall.getAccessMask());
      Assert.assertEquals(callTestData[i][1], nextCall.getType());
      Assert.assertEquals(callTestData[i][2], nextCall.getName());
      Assert.assertEquals((long) (Long) callTestData[i][3], nextCall.getGroupID());
      Assert.assertEquals(callTestData[i][4], nextCall.getDescription());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getCallListExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getCallListStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getCallListDetail());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < callGroupTestData.length; i++) {
      // Make half the existing data have unseen group IDs. These items should be removed during the sync
      // since they will not be returned from the API.
      CallGroup nextGroup = new CallGroup(
          i % 2 == 0 ? TestBase.getUniqueRandomLong() : (long) (Long) callGroupTestData[i][0], (String) callGroupTestData[i][1] + "1",
          (String) callGroupTestData[i][2] + "1");
      nextGroup.setup(testTime - 1);
      RefCachedData.updateData(nextGroup);
    }
    for (int i = 0; i < callTestData.length; i++) {
      // Make half the existing data have unseen types and names. These items should be removed during
      // the sync since they will not be returned from the API.
      Call nextCall = new Call(
          (Long) callTestData[i][0] + 1, (String) callTestData[i][1] + (i % 2 == 0 ? "1" : ""), (String) callTestData[i][2] + (i % 2 == 0 ? "1" : ""),
          (Long) callTestData[i][3] + 1, (String) callTestData[i][4] + "1");
      nextCall.setup(testTime - 1);
      RefCachedData.updateData(nextCall);
    }

    // Perform the sync
    SyncStatus syncOutcome = CallListSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<CallGroup> storedGroups = retrieveAll(new BatchRetriever<CallGroup>() {

      @Override
      public List<CallGroup> getNextBatch(
                                          List<CallGroup> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return CallGroup.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<Call> storedCalls = retrieveAll(new BatchRetriever<Call>() {

      @Override
      public List<Call> getNextBatch(
                                     List<Call> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return Call.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(callGroupTestData.length, storedGroups.size());
    Assert.assertEquals(callTestData.length, storedCalls.size());
    for (int i = 0; i < callGroupTestData.length; i++) {
      CallGroup nextGroup = storedGroups.get(i);
      Assert.assertEquals((long) (Long) callGroupTestData[i][0], nextGroup.getGroupID());
      Assert.assertEquals(callGroupTestData[i][1], nextGroup.getName());
      Assert.assertEquals(callGroupTestData[i][2], nextGroup.getDescription());
    }
    for (int i = 0; i < callTestData.length; i++) {
      Call nextCall = storedCalls.get(i);
      Assert.assertEquals((long) (Long) callTestData[i][0], nextCall.getAccessMask());
      Assert.assertEquals(callTestData[i][1], nextCall.getType());
      Assert.assertEquals(callTestData[i][2], nextCall.getName());
      Assert.assertEquals((long) (Long) callTestData[i][3], nextCall.getGroupID());
      Assert.assertEquals(callTestData[i][4], nextCall.getDescription());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getCallListExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getCallListStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getCallListDetail());
  }

  // Test skips update when already updated
  @Test
  public void testSyncUpdateSkip() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < callGroupTestData.length; i++) {
      CallGroup nextGroup = new CallGroup((Long) callGroupTestData[i][0] + 1, (String) callGroupTestData[i][1] + "1", (String) callGroupTestData[i][2] + "1");
      nextGroup.setup(testTime - 1);
      RefCachedData.updateData(nextGroup);
    }
    for (int i = 0; i < callTestData.length; i++) {
      Call nextCall = new Call(
          (Long) callTestData[i][0] + 1, (String) callTestData[i][1] + "1", (String) callTestData[i][2] + "1", (Long) callTestData[i][3] + 1,
          (String) callTestData[i][4] + "1");
      nextCall.setup(testTime - 1);
      RefCachedData.updateData(nextCall);
    }

    // Set the tracker as already updated and populate the container
    tracker.setCallListStatus(SyncState.UPDATED);
    tracker.setCallListDetail(null);
    tracker = RefSyncTracker.updateTracker(tracker);
    container.setCallListExpiry(prevDate);
    container = RefCachedData.updateData(container);

    // Perform the sync
    SyncStatus syncOutcome = CallListSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    // Skip the verify here since the calls should never be made

    // Verify unchanged
    List<CallGroup> storedGroups = retrieveAll(new BatchRetriever<CallGroup>() {

      @Override
      public List<CallGroup> getNextBatch(
                                          List<CallGroup> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return CallGroup.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<Call> storedCalls = retrieveAll(new BatchRetriever<Call>() {

      @Override
      public List<Call> getNextBatch(
                                     List<Call> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return Call.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(callGroupTestData.length, storedGroups.size());
    Assert.assertEquals(callTestData.length, storedCalls.size());
    for (int i = 0; i < callGroupTestData.length; i++) {
      CallGroup nextGroup = storedGroups.get(i);
      Assert.assertEquals((Long) callGroupTestData[i][0] + 1, nextGroup.getGroupID());
      Assert.assertEquals(callGroupTestData[i][1] + "1", nextGroup.getName());
      Assert.assertEquals(callGroupTestData[i][2] + "1", nextGroup.getDescription());
    }
    for (int i = 0; i < callTestData.length; i++) {
      Call nextCall = storedCalls.get(i);
      Assert.assertEquals((Long) callTestData[i][0] + 1, nextCall.getAccessMask());
      Assert.assertEquals(callTestData[i][1] + "1", nextCall.getType());
      Assert.assertEquals(callTestData[i][2] + "1", nextCall.getName());
      Assert.assertEquals((Long) callTestData[i][3] + 1, nextCall.getGroupID());
      Assert.assertEquals(callTestData[i][4] + "1", nextCall.getDescription());
    }

    // Verify tracker and container unchanged
    Assert.assertEquals(prevDate, RefData.getRefData().getCallListExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getCallListStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getCallListDetail());
  }

}
