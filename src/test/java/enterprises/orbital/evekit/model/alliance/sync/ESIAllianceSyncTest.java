package enterprises.orbital.evekit.model.alliance.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.AllianceApi;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.GetAlliancesAllianceIdIconsOk;
import enterprises.orbital.eve.esi.client.model.GetAlliancesAllianceIdOk;
import enterprises.orbital.evekit.TestBase;
import enterprises.orbital.evekit.account.EveKitRefDataProvider;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.alliance.Alliance;
import enterprises.orbital.evekit.model.alliance.AllianceIcon;
import enterprises.orbital.evekit.model.alliance.AllianceMemberCorporation;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ESIAllianceSyncTest extends RefTestBase {

  // Local mocks and other objects
  private ESIRefClientProvider mockServer;
  private AllianceApi mockEndpoint;
  private long testTime = 1238L;

  private static Object[][] allianceTestData;
  private static Object[][] allianceIconTestData;
  private static Object[][] allianceMemberTestData;

  static {
    // Alliance test data
    // 0 int allianceID;
    // 1 int executorCorpID;
    // 2 String name;
    // 3 String shortName;
    // 4 long startDate;
    // 5 int creatorID
    // 6 int creatorCorpID
    // 7 int factionID
    int size = 20 + TestBase.getRandomInt(20);
    allianceTestData = new Object[size][8];
    for (int i = 0; i < size; i++) {
      // Make sure alliance always ends in 0 to be included in the batch
      allianceTestData[i][0] = (TestBase.getUniqueRandomInteger() / 10) * 10;
      allianceTestData[i][1] = TestBase.getRandomInt();
      allianceTestData[i][2] = TestBase.getRandomText(50);
      allianceTestData[i][3] = TestBase.getRandomText(50);
      allianceTestData[i][4] = OrbitalProperties.getCurrentTime() + TestBase.getRandomInt(100);
      allianceTestData[i][5] = TestBase.getRandomInt();
      allianceTestData[i][6] = TestBase.getRandomInt();
      allianceTestData[i][7] = TestBase.getRandomInt();
    }
    // Alliance icon test data
    // 0 int allianceID
    // 1 String px64x64
    // 2 String px128x128
    allianceIconTestData = new Object[size][3];
    for (int i = 0; i < size; i++) {
      allianceIconTestData[i][0] = allianceTestData[i][0];
      allianceIconTestData[i][1] = TestBase.getRandomText(50);
      allianceIconTestData[i][2] = TestBase.getRandomText(50);
    }
    // Alliance member corp test data
    // 0 int allianceID;
    // 1 int corporationID;
    size = 100 + TestBase.getRandomInt(100);
    allianceMemberTestData = new Object[size][2];
    for (int i = 0, j = 0; i < size; i++, j = (j + 1) % allianceTestData.length) {
      allianceMemberTestData[i][0] = allianceTestData[j][0];
      allianceMemberTestData[i][1] = TestBase.getRandomInt();
    }

  }

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();

    // Prepare a test sync tracker
    ESIRefEndpointSyncTracker.getOrCreateUnfinishedTracker(ESIRefSyncEndpoint.REF_ALLIANCE, 1234L, "0");

    // Initialize time keeper
    OrbitalProperties.setTimeGenerator(() -> testTime);
  }

  @Override
  @After
  public void teardown() throws Exception {
    // Cleanup test specific tables after each test
    EveKitRefDataProvider.getFactory()
                         .runTransaction(() -> {
                           EveKitRefDataProvider.getFactory()
                                                .getEntityManager()
                                                .createQuery("DELETE FROM Alliance")
                                                .executeUpdate();
                           EveKitRefDataProvider.getFactory()
                                                .getEntityManager()
                                                .createQuery("DELETE FROM AllianceMemberCorporation ")
                                                .executeUpdate();
                           EveKitRefDataProvider.getFactory()
                                                .getEntityManager()
                                                .createQuery("DELETE FROM AllianceIcon")
                                                .executeUpdate();
                         });
    OrbitalProperties.setTimeGenerator(null);
    super.teardown();
  }

  // Mock up server interface
  private void setupMock() throws Exception {
    // Setup Alliance endpoint mock
    mockEndpoint = EasyMock.createMock(AllianceApi.class);

    // Setup alliance list call
    List<Integer> allianceList = new ArrayList<>();
    for (Object[] allianceData : allianceTestData) {
      allianceList.add((Integer) allianceData[0]);
    }
    Map<String, List<String>> headers = createHeaders("Expires", "Thu, 21 Dec 2017 12:00:00 GMT");
    ApiResponse<List<Integer>> allianceListResponse = new ApiResponse<>(200, headers, allianceList);
    EasyMock.expect(mockEndpoint.getAlliancesWithHttpInfo(null, null, null, null))
            .andReturn(allianceListResponse);

    // Setup calls to request individual alliance info
    for (Object[] testData : allianceTestData) {
      int allianceID = (Integer) testData[0];
      GetAlliancesAllianceIdOk allianceData = new GetAlliancesAllianceIdOk();
      allianceData.setExecutorCorporationId((Integer) testData[1]);
      allianceData.setName((String) testData[2]);
      allianceData.setTicker((String) testData[3]);
      allianceData.setDateFounded(new DateTime(new Date((Long) testData[4])));
      allianceData.setCreatorId((Integer) testData[5]);
      allianceData.setCreatorCorporationId((Integer) testData[6]);
      allianceData.setFactionId((Integer) testData[7]);
      ApiResponse<GetAlliancesAllianceIdOk> nextAllianceResponse = new ApiResponse<>(200, headers, allianceData);
      EasyMock.expect(mockEndpoint.getAlliancesAllianceIdWithHttpInfo(allianceID, null, null, null, null))
              .andReturn(nextAllianceResponse);
    }

    // Setup calls to request alliance icon info
    for (int i = 0; i < allianceTestData.length; i++) {
      int allianceID = (Integer) allianceTestData[i][0];
      GetAlliancesAllianceIdIconsOk iconData = new GetAlliancesAllianceIdIconsOk();
      iconData.setPx64x64((String) allianceIconTestData[i][1]);
      iconData.setPx128x128((String) allianceIconTestData[i][2]);
      ApiResponse<GetAlliancesAllianceIdIconsOk> nextIconResponse = new ApiResponse<>(200, headers, iconData);
      EasyMock.expect(mockEndpoint.getAlliancesAllianceIdIconsWithHttpInfo(allianceID, null, null, null, null))
              .andReturn(nextIconResponse);
    }

    // Setup calls to request alliance corporation lists
    for (Object[] allianceData : allianceTestData) {
      int allianceID = (Integer) allianceData[0];
      List<Integer> corpList = new ArrayList<>();
      for (Object[] memberData : allianceMemberTestData) {
        if (memberData[0].equals(allianceData[0])) {
          corpList.add((Integer) memberData[1]);
        }
      }
      ApiResponse<List<Integer>> nextCorpResponse = new ApiResponse<>(200, headers, corpList);
      EasyMock.expect(mockEndpoint.getAlliancesAllianceIdCorporationsWithHttpInfo(allianceID, null, null, null, null))
              .andReturn(nextCorpResponse);
    }

    // Setup an ExecutorService which immediately executes any submitted runnable
    ExecutorService immediateExecutor = Executors.newSingleThreadExecutor();

    // Finally, setup client provider mock
    mockServer = EasyMock.createMock(ESIRefClientProvider.class);
    EasyMock.expect(mockServer.getScheduler())
            .andReturn(immediateExecutor)
            .anyTimes();
    EasyMock.expect(mockServer.getAllianceApi())
            .andReturn(mockEndpoint)
            .anyTimes();
  }

  private void verifyDataUpdate() throws Exception {
    // Retrieve all stored alliance data (alliances, icons and members)
    List<Alliance> storedAlliances = AbstractESIRefSync.retrieveAll(testTime, (long contid, AttributeSelector at) ->
        Alliance.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR,
                             AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR,
                             AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR,
                             AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR,
                             AbstractESIRefSync.ANY_SELECTOR));

    List<AllianceIcon> storedIcons = AbstractESIRefSync.retrieveAll(testTime, (long contid, AttributeSelector at) ->
        AllianceIcon.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR,
                                 AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    List<AllianceMemberCorporation> storedMembers = AbstractESIRefSync.retrieveAll(testTime,
                                                                                   (long contid, AttributeSelector at) ->
                                                                                       AllianceMemberCorporation.accessQuery(
                                                                                           contid, 1000, false, at,
                                                                                           AbstractESIRefSync.ANY_SELECTOR,
                                                                                           AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(allianceTestData.length, storedAlliances.size());
    Assert.assertEquals(allianceIconTestData.length, storedIcons.size());
    Assert.assertEquals(allianceMemberTestData.length, storedMembers.size());

    // Check alliance data
    for (int i = 0; i < allianceTestData.length; i++) {
      Alliance nextAlliance = storedAlliances.get(i);
      long allianceID = nextAlliance.getAllianceID();
      Assert.assertEquals((int) (Integer) allianceTestData[i][0], nextAlliance.getAllianceID());
      Assert.assertEquals((int) (Integer) allianceTestData[i][1], nextAlliance.getExecutorCorpID());
      int memberCount = (int) Arrays.stream(allianceMemberTestData)
                                    .filter(x -> ((Integer) x[0]).longValue() == allianceID)
                                    .count();
      Assert.assertEquals(memberCount, nextAlliance.getMemberCount());
      Assert.assertEquals(allianceTestData[i][2], nextAlliance.getName());
      Assert.assertEquals(allianceTestData[i][3], nextAlliance.getShortName());
      Assert.assertEquals((long) (Long) allianceTestData[i][4], nextAlliance.getStartDate());
      Assert.assertEquals((int) (Integer) allianceTestData[i][5], nextAlliance.getCreatorID());
      Assert.assertEquals((int) (Integer) allianceTestData[i][6], nextAlliance.getCreatorCorpID());
      Assert.assertEquals((int) (Integer) allianceTestData[i][7], nextAlliance.getFactionID());
    }

    // Check alliance icon data
    for (int i = 0; i < allianceIconTestData.length; i++) {
      AllianceIcon nextIcon = storedIcons.get(i);
      Assert.assertEquals((int) (Integer) allianceIconTestData[i][0], nextIcon.getAllianceID());
      Assert.assertEquals(allianceIconTestData[i][1], nextIcon.getPx64x64());
      Assert.assertEquals(allianceIconTestData[i][2], nextIcon.getPx128x128());
    }

    // Check alliance member data
    for (int i = 0; i < allianceTestData.length; i++) {
      Alliance nextAlliance = storedAlliances.get(i);
      long allianceID = nextAlliance.getAllianceID();
      Set<Long> testCorpMembers = Arrays.stream(allianceMemberTestData)
                                        .filter(x -> ((Integer) x[0]).longValue() == allianceID)
                                        .map(x -> ((Integer) x[1]).longValue())
                                        .collect(Collectors.toSet());
      Set<Long> storedCorpMembers = storedMembers.stream()
                                                 .filter(x -> x.getAllianceID() == allianceID)
                                                 .map(AllianceMemberCorporation::getCorporationID)
                                                 .collect(Collectors.toSet());
      Assert.assertTrue(
          testCorpMembers.size() == storedCorpMembers.size() && testCorpMembers.containsAll(storedCorpMembers));
    }
  }

  @Test
  public void testSyncUpdate() throws Exception {
    setupMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Perform the sync
    ESIAllianceSync sync = new ESIAllianceSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify updated properly
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(
        ESIRefSyncEndpoint.REF_ALLIANCE);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_ALLIANCE);
    long schedTime = testTime + TimeUnit.MILLISECONDS.convert(6, TimeUnit.MINUTES);
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Populate existing
    //
    // Half the alliances we insert are purposely not present in the server data so we can test deletion.
    // Compute the modified alliance IDs and member counts first
    int[] modifiedAllianceIDs = new int[allianceTestData.length];
    for (int i = 0; i < modifiedAllianceIDs.length; i++) {
      if (i % 2 == 0)
        modifiedAllianceIDs[i] = TestBase.getUniqueRandomInteger();
      else
        modifiedAllianceIDs[i] = (int) (Integer) allianceTestData[i][0];
    }
    int[] memberCounts = new int[allianceTestData.length];
    for (int i = 0; i < memberCounts.length; i++) {
      int allianceID = (int) (Integer) allianceTestData[i][0];
      memberCounts[i] = (int) Arrays.stream(allianceMemberTestData)
                                    .filter(x -> allianceID == (Integer) x[0])
                                    .count();
    }
    // Populate existing alliances and icons
    for (int i = 0; i < allianceTestData.length; i++) {
      Alliance newAlliance = new Alliance(modifiedAllianceIDs[i], (Integer) allianceTestData[i][1] + 1,
                                          memberCounts[i], allianceTestData[i][2] + "1", allianceTestData[i][3] + "1",
                                          (Long) allianceTestData[i][4] + 1, (Integer) allianceTestData[i][5] + 1,
                                          (Integer) allianceTestData[i][6] + 1, (Integer) allianceTestData[i][7] + 1);
      newAlliance.setup(testTime - 1);
      RefCachedData.update(newAlliance);

      AllianceIcon newIcon = new AllianceIcon(modifiedAllianceIDs[i], allianceIconTestData[i][1] + "1",
                                              allianceIconTestData[i][2] + "1");
      newIcon.setup(testTime - 1);
      RefCachedData.update(newIcon);
    }
    // Populate existing group membership
    for (int i = 0; i < allianceMemberTestData.length; i++) {
      // Make half the existing data have unseen corporation IDs. These items should be removed during the sync
      // since they will not be returned from the API.
      int nextAllianceID = modifiedAllianceIDs[i % modifiedAllianceIDs.length];
      AllianceMemberCorporation newMember = new AllianceMemberCorporation(nextAllianceID,
                                                                          (Integer) allianceMemberTestData[i][1] + 1);
      newMember.setup(testTime - 1);
      RefCachedData.update(newMember);
    }

    // Perform the sync
    ESIAllianceSync sync = new ESIAllianceSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify old objects were evolved properly
    List<Alliance> oldAlliances = AbstractESIRefSync.retrieveAll(testTime - 1, (long contid, AttributeSelector at) ->
        Alliance.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR,
                             AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR,
                             AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR,
                             AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR,
                             AbstractESIRefSync.ANY_SELECTOR));

    List<AllianceIcon> oldIcons = AbstractESIRefSync.retrieveAll(testTime - 1, (long contid, AttributeSelector at) ->
        AllianceIcon.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR,
                                 AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    List<AllianceMemberCorporation> oldMembers = AbstractESIRefSync.retrieveAll(testTime - 1,
                                                                                (long contid, AttributeSelector at) ->
                                                                                    AllianceMemberCorporation.accessQuery(
                                                                                        contid, 1000, false, at,
                                                                                        AbstractESIRefSync.ANY_SELECTOR,
                                                                                        AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(allianceTestData.length, oldAlliances.size());
    Assert.assertEquals(allianceIconTestData.length, oldIcons.size());
    Assert.assertEquals(allianceMemberTestData.length, oldMembers.size());

    // Check old alliance data
    for (int i = 0; i < allianceTestData.length; i++) {
      Alliance nextAlliance = oldAlliances.get(i);
      Assert.assertEquals(testTime, nextAlliance.getLifeEnd());
      Assert.assertEquals(modifiedAllianceIDs[i], nextAlliance.getAllianceID());
      Assert.assertEquals((Integer) allianceTestData[i][1] + 1, nextAlliance.getExecutorCorpID());
      Assert.assertEquals(memberCounts[i], nextAlliance.getMemberCount());
      Assert.assertEquals(allianceTestData[i][2] + "1", nextAlliance.getName());
      Assert.assertEquals(allianceTestData[i][3] + "1", nextAlliance.getShortName());
      Assert.assertEquals((Long) allianceTestData[i][4] + 1, nextAlliance.getStartDate());
      Assert.assertEquals((Integer) allianceTestData[i][5] + 1, nextAlliance.getCreatorID());
      Assert.assertEquals((Integer) allianceTestData[i][6] + 1, nextAlliance.getCreatorCorpID());
      Assert.assertEquals((Integer) allianceTestData[i][7] + 1, nextAlliance.getFactionID());
    }

    // Check alliance icon data
    for (int i = 0; i < allianceIconTestData.length; i++) {
      AllianceIcon nextIcon = oldIcons.get(i);
      Assert.assertEquals(testTime, nextIcon.getLifeEnd());
      Assert.assertEquals(modifiedAllianceIDs[i], nextIcon.getAllianceID());
      Assert.assertEquals(allianceIconTestData[i][1] + "1", nextIcon.getPx64x64());
      Assert.assertEquals(allianceIconTestData[i][2] + "1", nextIcon.getPx128x128());
    }

    // Check alliance member data
    for (AllianceMemberCorporation nextCorp : oldMembers)
      Assert.assertEquals(testTime, nextCorp.getLifeEnd());
    for (int i = 0; i < allianceTestData.length; i++) {
      Alliance nextAlliance = oldAlliances.get(i);
      long allianceID = nextAlliance.getAllianceID();
      Set<Long> testCorpMembers = new HashSet<>();
      for (int j = 0; j < allianceMemberTestData.length; j++) {
        if (modifiedAllianceIDs[j % allianceTestData.length] == allianceID)
          testCorpMembers.add((long) (Integer) allianceMemberTestData[j][1] + 1);
      }
      Set<Long> storedCorpMembers = oldMembers.stream()
                                              .filter(x -> x.getAllianceID() == allianceID)
                                              .map(AllianceMemberCorporation::getCorporationID)
                                              .collect(Collectors.toSet());
      Assert.assertEquals(testCorpMembers.size(), storedCorpMembers.size());
      Assert.assertTrue(testCorpMembers.containsAll(storedCorpMembers));
    }

    // Verify updates which will also verify that all old alliances were properly end of lifed
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(
        ESIRefSyncEndpoint.REF_ALLIANCE);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_ALLIANCE);
    long schedTime = testTime + TimeUnit.MILLISECONDS.convert(6, TimeUnit.MINUTES);
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

}
