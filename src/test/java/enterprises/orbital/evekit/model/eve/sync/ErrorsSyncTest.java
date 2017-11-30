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
import enterprises.orbital.evekit.model.eve.ErrorType;
import enterprises.orbital.evexmlapi.eve.IError;
import enterprises.orbital.evexmlapi.eve.IEveAPI;

public class ErrorsSyncTest extends RefTestBase {

  // Local mocks and other objects
  long                testDate;
  long                prevDate;
  RefData             container;
  RefSyncTracker      tracker;
  RefSynchronizerUtil syncUtil;
  IEveAPI             mockServer;

  static Object[][]   errorTestData;

  static {
    // ErrorType test data
    // 0 int errorCode
    // 1 String errorText
    int size = 20 + TestBase.getRandomInt(20);
    errorTestData = new Object[size][2];
    for (int i = 0; i < size; i++) {
      errorTestData[i][0] = TestBase.getUniqueRandomInteger();
      errorTestData[i][1] = TestBase.getRandomText(50);
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
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM ErrorType").executeUpdate();
      }
    });

    super.teardown();
  }

  // Mock up server interface
  public void setupOkMock() throws Exception {
    mockServer = EasyMock.createMock(IEveAPI.class);
    final List<IError> errors = new ArrayList<>();
    for (int i = 0; i < errorTestData.length; i++) {
      final Object[] data = errorTestData[i];
      errors.add(new IError() {

        @Override
        public int getErrorCode() {
          return (Integer) data[0];
        }

        @Override
        public String getErrorText() {
          return (String) data[1];
        }

      });
    }
    EasyMock.expect(mockServer.requestErrors()).andReturn(errors);
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
    SyncStatus syncOutcome = ErrorsSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<ErrorType> storedErrors = retrieveAll(new BatchRetriever<ErrorType>() {

      @Override
      public List<ErrorType> getNextBatch(
                                          List<ErrorType> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return ErrorType.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(errorTestData.length, storedErrors.size());
    for (int i = 0; i < errorTestData.length; i++) {
      ErrorType nextError = storedErrors.get(i);
      Assert.assertEquals((int) (Integer) errorTestData[i][0], nextError.getErrorCode());
      Assert.assertEquals(errorTestData[i][1], nextError.getErrorText());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getErrorListExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getErrorListStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getErrorListDetail());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < errorTestData.length; i++) {
      // Make half the existing data have unseen error codes. These items should be removed during the sync
      // since they will not be returned from the API.
      ErrorType newError = new ErrorType(i % 2 == 0 ? TestBase.getUniqueRandomInteger() : (int) (Integer) errorTestData[i][0], errorTestData[i][1] + "1");
      newError.setup(testTime - 1);
      RefCachedData.updateData(newError);
    }

    // Perform the sync
    SyncStatus syncOutcome = ErrorsSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<ErrorType> storedErrors = retrieveAll(new BatchRetriever<ErrorType>() {

      @Override
      public List<ErrorType> getNextBatch(
                                          List<ErrorType> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return ErrorType.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(errorTestData.length, storedErrors.size());
    for (int i = 0; i < errorTestData.length; i++) {
      ErrorType nextError = storedErrors.get(i);
      Assert.assertEquals((int) (Integer) errorTestData[i][0], nextError.getErrorCode());
      Assert.assertEquals(errorTestData[i][1], nextError.getErrorText());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getErrorListExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getErrorListStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getErrorListDetail());
  }

  // Test skips update when already updated
  @Test
  public void testSyncUpdateSkip() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < errorTestData.length; i++) {
      // Make half the existing data have unseen error codes. These items should be removed during the sync
      // since they will not be returned from the API.
      ErrorType newError = new ErrorType((Integer) errorTestData[i][0] + 1, errorTestData[i][1] + "1");
      newError.setup(testTime - 1);
      RefCachedData.updateData(newError);
    }

    // Set the tracker as already updated and populate the container
    tracker.setErrorListStatus(SyncState.UPDATED);
    tracker.setErrorListDetail(null);
    tracker = RefSyncTracker.updateTracker(tracker);
    container.setErrorListExpiry(prevDate);
    container = RefCachedData.updateData(container);

    // Perform the sync
    SyncStatus syncOutcome = ErrorsSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    // Skip the verify here since the calls should never be made

    // Verify unchanged
    List<ErrorType> storedErrors = retrieveAll(new BatchRetriever<ErrorType>() {

      @Override
      public List<ErrorType> getNextBatch(
                                          List<ErrorType> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return ErrorType.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(errorTestData.length, storedErrors.size());
    for (int i = 0; i < errorTestData.length; i++) {
      ErrorType nextError = storedErrors.get(i);
      Assert.assertEquals((Integer) errorTestData[i][0] + 1, nextError.getErrorCode());
      Assert.assertEquals(errorTestData[i][1] + "1", nextError.getErrorText());
    }

    // Verify tracker and container unchanged
    Assert.assertEquals(prevDate, RefData.getRefData().getErrorListExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getErrorListStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getErrorListDetail());
  }

}
