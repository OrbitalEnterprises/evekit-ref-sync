package enterprises.orbital.evekit.model;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.invoker.ApiException;
import enterprises.orbital.evekit.TestBase;
import enterprises.orbital.evekit.account.EveKitRefDataProvider;
import enterprises.orbital.evekit.model.alliance.AllianceMemberCorporation;
import enterprises.orbital.evekit.model.server.ServerStatus;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static enterprises.orbital.evekit.model.AbstractESIRefSync.ANY_SELECTOR;

public class AbstractESIRefSyncTest extends RefTestBase {
  // We base our test on the ServerStatus endpoint, although any endpoint
  // could be used here.

  private final int SIM_MAX_DELAY = 1;
  private final int SIM_MISSING_PREREQS = 1 << 1;
  private final int SIM_API_ERROR = 1 << 2;
  private final int SIM_COMMIT_ERROR = 1 << 3;
  private final int SIM_PROCESS_ERROR = 1 << 4;

  protected long testTime = 1238L;

  // Concrete extension of abstract class so we can properly test.
  public class ClassUnderTest extends AbstractESIRefSync<Object> {

    int simMask;

    ClassUnderTest(int simMask) {
      this.simMask = simMask;
    }

    public long maxDelay() {
      return (simMask & SIM_MAX_DELAY) > 0 ? 2L : 1000L;
    }

    protected long defaultNextEvent() {
      return 1240L;
    }

    protected boolean prereqSatisfied() {
      return ! ((simMask & SIM_MISSING_PREREQS) > 0);
    }

    protected void commit(
        long time,
        RefCachedData item) throws IOException {
      if ((simMask & SIM_COMMIT_ERROR) > 0) throw new IOException();
    }

    @Override
    protected ESIRefServerResult<Object> getServerData(ESIRefClientProvider cp) throws ApiException, IOException {
      if ((simMask & SIM_API_ERROR) > 0)
        throw new ApiException();
      else
        return new ESIRefServerResult<>(1238L, null);
    }

    @Override
    protected void processServerData(long time, ESIRefServerResult<Object> data,
                                     List<RefCachedData> updates) throws IOException {
      if ((simMask & SIM_PROCESS_ERROR) > 0) throw new IOException();
      if ((simMask & SIM_COMMIT_ERROR) > 0) {
        // We can't trigger the commit error unless we have at least one thing to commit
        updates.add(new ServerStatus(10, 1234L, "112233", false));
      }
    }

    @Override
    public ESIRefSyncEndpoint endpoint() {
      return ESIRefSyncEndpoint.REF_SERVER_STATUS;
    }
  }

  // Standard tracker
  private ESIRefEndpointSyncTracker      tracker;

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();

    // Prepare a test sync tracker
    tracker = ESIRefEndpointSyncTracker.getOrCreateUnfinishedTracker(ESIRefSyncEndpoint.REF_SERVER_STATUS, 1234L, null);
  }

  @Override
  @After
  public void teardown() throws Exception {
    // retrieveAll test pollutes the DB so clean that up
    EveKitRefDataProvider.getFactory()
                         .runTransaction(() -> {
                           EveKitRefDataProvider.getFactory()
                                                .getEntityManager()
                                                .createQuery("DELETE FROM AllianceMemberCorporation ")
                                                .executeUpdate();
                         });
    // Reset time generator
    OrbitalProperties.setTimeGenerator(null);
    super.teardown();
  }

  @Test
  public void testNoSyncIfRefreshed() throws Exception {
    // Set the tracker as already updated
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_SERVER_STATUS);
    syncTracker.setSyncStart(testTime);
    syncTracker.setStatus(ESISyncState.FINISHED);
    EveKitRefDataProvider.update(syncTracker);

    // Perform the sync
    ESIRefClientProvider mockServer = EasyMock.createMock(ESIRefClientProvider.class);
    EasyMock.replay(mockServer);
    ClassUnderTest cut = new ClassUnderTest(0);
    cut.synch(mockServer);

    // Verify tracker unchanged
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_SERVER_STATUS);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals(null, syncTracker.getDetail());
    Assert.assertEquals(-1L, syncTracker.getSyncEnd());
  }

  @Test
  public void testKillStaleTracker() throws Exception {
    // Set the tracker as already updated
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_SERVER_STATUS);
    syncTracker.setSyncStart(1234L);
    EveKitRefDataProvider.update(syncTracker);

    // Perform the sync
    ESIRefClientProvider mockServer = EasyMock.createMock(ESIRefClientProvider.class);
    EasyMock.replay(mockServer);
    ClassUnderTest cut = new ClassUnderTest(SIM_MAX_DELAY);
    OrbitalProperties.setTimeGenerator(() -> testTime);
    cut.synch(mockServer);

    // Verify tracker is properly terminated
    syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_SERVER_STATUS);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(1234L, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.WARNING, syncTracker.getStatus());
    Assert.assertEquals("Terminated due to excessive delay", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify no new tracker has been created (sync manager responsible for creating in this case)
    try {
      ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_SERVER_STATUS);
      Assert.fail("unfinished tracker should not exist");
    } catch (TrackerNotFoundException e) {
      // expected
    }
  }

  @Test
  public void testNoSyncIfNoPreReqs() throws Exception {
    // Perform the sync with a synchronizer with unsatisfied pre-reqs
    ESIRefClientProvider mockServer = EasyMock.createMock(ESIRefClientProvider.class);
    EasyMock.replay(mockServer);
    ClassUnderTest cut = new ClassUnderTest(SIM_MISSING_PREREQS);
    cut.synch(mockServer);

    // Verify tracker not modified
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_SERVER_STATUS);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(-1L, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.NOT_PROCESSED, syncTracker.getStatus());
    Assert.assertEquals(null, syncTracker.getDetail());
    Assert.assertEquals(-1L, syncTracker.getSyncEnd());
  }

  private void checkForScheduledTracker(long scheduleTime) throws Exception {
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_SERVER_STATUS);
    Assert.assertEquals(scheduleTime, syncTracker.getScheduled());
    Assert.assertEquals(-1L, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.NOT_PROCESSED, syncTracker.getStatus());
    Assert.assertEquals(null, syncTracker.getDetail());
    Assert.assertEquals(-1L, syncTracker.getSyncEnd());
  }

  @Test
  public void testAPIExceptionHandled() throws Exception {
    // Perform the sync
    ESIRefClientProvider mockServer = EasyMock.createMock(ESIRefClientProvider.class);
    EasyMock.replay(mockServer);
    ClassUnderTest cut = new ClassUnderTest(SIM_API_ERROR);
    OrbitalProperties.setTimeGenerator(() -> testTime);
    cut.synch(mockServer);

    // Verify tracker is properly terminated
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_SERVER_STATUS);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.ERROR, syncTracker.getStatus());
    Assert.assertEquals("ESI client error, contact the site admin if this problem persists", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify a new tracker has been created and scheduled for the default time
    checkForScheduledTracker(1240L);
  }

  private void checkIOExceptionHandled() throws Exception {
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_SERVER_STATUS);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.ERROR, syncTracker.getStatus());
    Assert.assertEquals("Server error, contact the site admin if this problem persists", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());
  }

  @SuppressWarnings("Duplicates")
  @Test
  public void testCommitIOExceptionHandled() throws Exception {
    // Perform the sync
    ESIRefClientProvider mockServer = EasyMock.createMock(ESIRefClientProvider.class);
    EasyMock.replay(mockServer);
    ClassUnderTest cut = new ClassUnderTest(SIM_COMMIT_ERROR);
    OrbitalProperties.setTimeGenerator(() -> testTime);
    cut.synch(mockServer);

    // Verify tracker is properly terminated
    checkIOExceptionHandled();

    // Verify a new tracker has been created and scheduled for the default time
    checkForScheduledTracker(1240L);
  }

  @SuppressWarnings("Duplicates")
  @Test
  public void testProcessIOExceptionHandled() throws Exception {
    // Perform the sync
    ESIRefClientProvider mockServer = EasyMock.createMock(ESIRefClientProvider.class);
    EasyMock.replay(mockServer);
    ClassUnderTest cut = new ClassUnderTest(SIM_PROCESS_ERROR);
    OrbitalProperties.setTimeGenerator(() -> testTime);
    cut.synch(mockServer);

    // Verify tracker is properly terminated
    checkIOExceptionHandled();

    // Verify a new tracker has been created and scheduled for the default time
    checkForScheduledTracker(1240L);
  }

  @Test
  public void testRetrieveAll() throws Exception {
    // Create at least 1000 AllianceMemberCorporations live at a given time
    int objCount = 1010 + TestBase.getRandomInt(1000);
    long allianceID = TestBase.getUniqueRandomLong();
    List<AllianceMemberCorporation> testObjs = new ArrayList<>();
    for (int i = 0; i < objCount; i++) {
      AllianceMemberCorporation next = new AllianceMemberCorporation(allianceID, TestBase.getUniqueRandomLong());
      next.setup(testTime);
      testObjs.add(RefCachedData.update(next));
    }

    // Now setup a retrievAll query to retrieve all the objects we just created.
    List<AllianceMemberCorporation> stored = AbstractESIRefSync.retrieveAll(testTime,
                                                                            (long contid, AttributeSelector at) ->
                                                                                AllianceMemberCorporation.accessQuery(contid, 100, false, at, ANY_SELECTOR, ANY_SELECTOR));
    Assert.assertEquals(testObjs, stored);
  }
}
