package enterprises.orbital.evekit.model;

import java.io.IOException;
import java.util.List;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import enterprises.orbital.evekit.model.AbstractRefSync;
import enterprises.orbital.evekit.model.RefCachedData;
import enterprises.orbital.evekit.model.RefData;
import enterprises.orbital.evekit.model.RefSyncTracker;
import enterprises.orbital.evekit.model.RefSynchronizerUtil;
import enterprises.orbital.evekit.model.RefSynchronizerUtil.SyncStatus;
import enterprises.orbital.evekit.model.SyncTracker;
import enterprises.orbital.evekit.model.SyncTracker.SyncState;
import enterprises.orbital.evexmlapi.IResponse;

public class AbstractRefSyncTest extends SyncTestBase {

  // Concrete extension of abstract class so we can properly test.
  public class ClassUnderTest extends AbstractRefSync {

    @Override
    public boolean isRefreshed(
                               RefSyncTracker tracker) {
      return false;
    }

    @Override
    public void updateStatus(
                             RefSyncTracker tracker,
                             SyncState status,
                             String detail) {}

    @Override
    public void updateExpiry(
                             RefData container,
                             long expiry) {}

    @Override
    public long getExpiryTime(
                              RefData container) {
      return -1;
    }

    @Override
    protected Object getServerData(
                                   IResponse response)
      throws IOException {
      return null;
    }

    @Override
    protected long processServerData(
                                     long time,
                                     IResponse request,
                                     Object data,
                                     List<RefCachedData> updates)
      throws IOException {
      return -1;
    }

  }

  // Local mocks and other objects
  RefData             container;
  RefSyncTracker      tracker;
  RefSynchronizerUtil syncUtil;
  IResponse           mockServer;

  // Mock up server interface
  @Override
  @Before
  public void setup() throws Exception {
    super.setup();

    // Prepare a test sync tracker
    tracker = RefSyncTracker.createOrGetUnfinishedTracker();

    // Prepare a container
    container = RefData.getOrCreateRefData();

    // Prepare the synchronizer util
    syncUtil = new RefSynchronizerUtil();
  }

  @Test
  public void testSynchronizerUtilReturnsNonZero() {
    ClassUnderTest cut = new ClassUnderTest();
    IResponse mockRequest = EasyMock.createMock(IResponse.class);
    RefSynchronizerUtil mockUtil = EasyMock.createMock(RefSynchronizerUtil.class);
    EasyMock.expect(mockUtil.preSyncCheck("SyncUnitTest", cut)).andReturn(RefSynchronizerUtil.SyncStatus.ERROR);
    EasyMock.replay(mockUtil);
    RefSynchronizerUtil.SyncStatus result = cut.syncData(1234L, mockUtil, mockRequest, "SyncUnitTest");
    Assert.assertEquals(SyncStatus.ERROR, result);
    EasyMock.verify(mockUtil);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRequestIsError() throws Exception {
    int errorCode = 100;
    long testTime = 1234L;
    String errorText = "Error Text";
    String errorString = "Error " + errorCode + ": " + errorText;
    ClassUnderTest cut = new ClassUnderTest();
    IResponse mockRequest = EasyMock.createMock(IResponse.class);
    RefSynchronizerUtil mockUtil = EasyMock.createMock(RefSynchronizerUtil.class);
    EasyMock.expect(mockUtil.preSyncCheck("SyncUnitTest", cut)).andReturn(RefSynchronizerUtil.SyncStatus.CONTINUE);
    EasyMock.expect(mockRequest.isError()).andReturn(true);
    EasyMock.expect(mockRequest.getErrorCode()).andReturn(errorCode).times(2);
    EasyMock.expect(mockRequest.getErrorString()).andReturn(errorText);
    mockUtil.storeSynchResults(EasyMock.eq(testTime), EasyMock.eq(SyncTracker.SyncState.SYNC_ERROR), EasyMock.eq(errorString), EasyMock.eq(-1L),
                               EasyMock.eq("SyncUnitTest"), (List<RefCachedData>) EasyMock.notNull(), EasyMock.eq(cut));
    EasyMock.replay(mockUtil);
    EasyMock.replay(mockRequest);
    RefSynchronizerUtil.SyncStatus result = cut.syncData(testTime, mockUtil, mockRequest, "SyncUnitTest");
    Assert.assertEquals(SyncStatus.DONE, result);
    EasyMock.verify(mockUtil);
    EasyMock.verify(mockRequest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRequestThrowsIOException() throws IOException {
    long testTime = 1234L;
    ClassUnderTest extended = new ClassUnderTest() {

      @Override
      protected Object getServerData(
                                     IResponse request)
        throws IOException {
        throw new IOException("Test Exception");
      }

    };

    String errorString = "request failed with IO error";
    IResponse mockRequest = EasyMock.createMock(IResponse.class);
    RefSynchronizerUtil mockUtil = EasyMock.createMock(RefSynchronizerUtil.class);
    EasyMock.expect(mockUtil.preSyncCheck("SyncUnitTest", extended)).andReturn(RefSynchronizerUtil.SyncStatus.CONTINUE);
    mockUtil.storeSynchResults(EasyMock.eq(testTime), EasyMock.eq(SyncTracker.SyncState.SYNC_ERROR), EasyMock.eq(errorString), EasyMock.eq(-1L),
                               EasyMock.eq("SyncUnitTest"), (List<RefCachedData>) EasyMock.notNull(), EasyMock.eq(extended));
    EasyMock.replay(mockUtil);
    EasyMock.replay(mockRequest);
    RefSynchronizerUtil.SyncStatus result = extended.syncData(testTime, mockUtil, mockRequest, "SyncUnitTest");
    Assert.assertEquals(SyncStatus.DONE, result);
    EasyMock.verify(mockUtil);
    EasyMock.verify(mockRequest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testStoreSyncThrowsIOException() throws IOException {
    long testTime = 1234L;
    ClassUnderTest cut = new ClassUnderTest();
    IResponse mockRequest = EasyMock.createMock(IResponse.class);
    RefSynchronizerUtil mockUtil = EasyMock.createMock(RefSynchronizerUtil.class);
    EasyMock.expect(mockUtil.preSyncCheck("SyncUnitTest", cut)).andReturn(RefSynchronizerUtil.SyncStatus.CONTINUE);
    EasyMock.expect(mockRequest.isError()).andReturn(false);
    mockUtil.storeSynchResults(EasyMock.eq(testTime), EasyMock.eq(SyncTracker.SyncState.UPDATED), (String) EasyMock.isNull(), EasyMock.eq(-1L),
                               EasyMock.eq("SyncUnitTest"), (List<RefCachedData>) EasyMock.notNull(), EasyMock.eq(cut));
    EasyMock.expectLastCall().andThrow(new IOException("Test Exception"));
    EasyMock.replay(mockUtil);
    EasyMock.replay(mockRequest);
    RefSynchronizerUtil.SyncStatus result = cut.syncData(testTime, mockUtil, mockRequest, "SyncUnitTest");
    Assert.assertEquals(SyncStatus.ERROR, result);
    EasyMock.verify(mockUtil);
    EasyMock.verify(mockRequest);
  }

}
