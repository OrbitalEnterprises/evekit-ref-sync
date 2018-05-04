package enterprises.orbital.evekit.model.sov.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.SovereigntyApi;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.GetSovereigntyCampaigns200Ok;
import enterprises.orbital.eve.esi.client.model.GetSovereigntyCampaignsParticipant;
import enterprises.orbital.evekit.TestBase;
import enterprises.orbital.evekit.account.EveKitRefDataProvider;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.sov.SovereigntyCampaign;
import enterprises.orbital.evekit.model.sov.SovereigntyCampaignParticipant;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

public class ESISovereigntyCampaignSyncTest extends RefTestBase {

  // Local mocks and other objects
  private ESIRefClientProvider mockServer;
  private SovereigntyApi mockEndpoint;
  private long testTime = 1238L;

  private static Object[][]   sovereigntyCampaignTestData;
  private static Object[][]   sovereigntyCampaignParticipantTestData;

  static {
    // Sovereignty campaign test data
    // 0 int campaignID
    // 1 long structureID
    // 2 int systemID
    // 3 int constellationID
    // 4 String eventType
    // 5 long startTime
    // 6 int defenderID
    // 7 float defenderScore
    // 8 float attackersScore
    int size = 20 + TestBase.getRandomInt(20);
    sovereigntyCampaignTestData = new Object[size][9];
    int eventTypeRange = GetSovereigntyCampaigns200Ok.EventTypeEnum.values().length;
    for (int i = 0; i < size; i++) {
      sovereigntyCampaignTestData[i][0] = TestBase.getUniqueRandomInteger();
      sovereigntyCampaignTestData[i][1] = TestBase.getRandomLong();
      sovereigntyCampaignTestData[i][2] = TestBase.getRandomInt();
      sovereigntyCampaignTestData[i][3] = TestBase.getRandomInt();
      sovereigntyCampaignTestData[i][4] = GetSovereigntyCampaigns200Ok.EventTypeEnum.values()[TestBase.getRandomInt(eventTypeRange)].toString();
      sovereigntyCampaignTestData[i][5] = OrbitalProperties.getCurrentTime() + TestBase.getRandomInt(100);
      sovereigntyCampaignTestData[i][6] = TestBase.getRandomInt();
      sovereigntyCampaignTestData[i][7] = (float) TestBase.getRandomDouble(1000);
      sovereigntyCampaignTestData[i][8] = (float) TestBase.getRandomDouble(1000);
    }
    // Sovereignty campaign participant test data
    // 0 int campaignID
    // 1 int allianceID
    // 2 float score
    size = 100 + TestBase.getRandomInt(100);
    sovereigntyCampaignParticipantTestData = new Object[size][3];
    for (int i = 0, j = 0; i < size; i++, j = (j + 1) % sovereigntyCampaignTestData.length) {
      sovereigntyCampaignParticipantTestData[i][0] = sovereigntyCampaignTestData[j][0];
      sovereigntyCampaignParticipantTestData[i][1] = TestBase.getRandomInt();
      sovereigntyCampaignParticipantTestData[i][2] = (float) TestBase.getRandomDouble(1000);
    }
  }

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();

    // Prepare a test sync tracker
    ESIRefEndpointSyncTracker.getOrCreateUnfinishedTracker(ESIRefSyncEndpoint.REF_SOV_CAMPAIGN, 1234L, null);

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
                                                .createQuery("DELETE FROM SovereigntyCampaign ")
                                                .executeUpdate();
                           EveKitRefDataProvider.getFactory()
                                                .getEntityManager()
                                                .createQuery("DELETE FROM SovereigntyCampaignParticipant ")
                                                .executeUpdate();
                         });
    OrbitalProperties.setTimeGenerator(null);
    super.teardown();
  }

  private GetSovereigntyCampaigns200Ok.EventTypeEnum mapStringToEnum(String src) {
    for (GetSovereigntyCampaigns200Ok.EventTypeEnum next : GetSovereigntyCampaigns200Ok.EventTypeEnum.values()) {
      if (next.toString().equals(src)) return next;
    }
    throw new IllegalArgumentException("Can't map " + src + " to an enum type");
  }

  // Mock up server interface
  private void setupMock() throws Exception {
    // Setup Sovereignty endpoint mock
    mockEndpoint = EasyMock.createMock(SovereigntyApi.class);

    // Setup sovereignty campaign list call
    List<GetSovereigntyCampaigns200Ok> campaignList = new ArrayList<>();
    for (Object[] nextCampaign : sovereigntyCampaignTestData) {
      int campaignID = (Integer) nextCampaign[0];
      GetSovereigntyCampaigns200Ok next = new GetSovereigntyCampaigns200Ok();
      next.setCampaignId(campaignID);
      next.setStructureId((Long) nextCampaign[1]);
      next.setSolarSystemId((Integer) nextCampaign[2]);
      next.setConstellationId((Integer) nextCampaign[3]);
      next.setEventType(mapStringToEnum((String) nextCampaign[4]));
      next.setStartTime(new DateTime(new Date((Long) nextCampaign[5])));
      next.setDefenderId((Integer) nextCampaign[6]);
      next.setDefenderScore((Float) nextCampaign[7]);
      next.setAttackersScore((Float) nextCampaign[8]);

      // Compute and set participants
      List<GetSovereigntyCampaignsParticipant> partList = Arrays.stream(sovereigntyCampaignParticipantTestData)
                                                                .filter(x -> campaignID == (Integer) x[0])
                                                                .map(x -> {
                                                                  GetSovereigntyCampaignsParticipant newPart = new GetSovereigntyCampaignsParticipant();
                                                                  newPart.setAllianceId((Integer) x[1]);
                                                                  newPart.setScore((Float) x[2]);
                                                                  return newPart;
                                                                })
                                                                .collect(Collectors.toList());
      next.setParticipants(partList);

      campaignList.add(next);
    }

    Map<String, List<String>> headers = createHeaders("Expires", "Thu, 21 Dec 2017 12:00:00 GMT");
    ApiResponse<List<GetSovereigntyCampaigns200Ok>> campaignListResponse = new ApiResponse<>(200, headers, campaignList);
    EasyMock.expect(mockEndpoint.getSovereigntyCampaignsWithHttpInfo(null, null, null, null)).andReturn(campaignListResponse);

    // Finally, setup client provider mock
    mockServer = EasyMock.createMock(ESIRefClientProvider.class);
    EasyMock.expect(mockServer.getSovereigntyApi()).andReturn(mockEndpoint).anyTimes();
  }

  private void verifyDataUpdate() throws Exception {
    // Retrieve all stored sovereignty campagin data (campaigns and participants)
    List<SovereigntyCampaign> storedCampaigns = AbstractESIRefSync.retrieveAll(testTime, (long contid, AttributeSelector at) ->
        SovereigntyCampaign.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR,
                             AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    List<SovereigntyCampaignParticipant> storedParts = AbstractESIRefSync.retrieveAll(testTime, (long contid, AttributeSelector at) ->
        SovereigntyCampaignParticipant.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(sovereigntyCampaignTestData.length, storedCampaigns.size());
    Assert.assertEquals(sovereigntyCampaignParticipantTestData.length, storedParts.size());

    // Check sovereignty campaign data
    for (int i = 0; i < sovereigntyCampaignTestData.length; i++) {
      SovereigntyCampaign nextCampaign = storedCampaigns.get(i);
      Assert.assertEquals((int) (Integer) sovereigntyCampaignTestData[i][0], nextCampaign.getCampaignID());
      Assert.assertEquals((long) (Long) sovereigntyCampaignTestData[i][1], nextCampaign.getStructureID());
      Assert.assertEquals((int) (Integer) sovereigntyCampaignTestData[i][2], nextCampaign.getSystemID());
      Assert.assertEquals((int) (Integer) sovereigntyCampaignTestData[i][3], nextCampaign.getConstellationID());
      Assert.assertEquals(sovereigntyCampaignTestData[i][4], nextCampaign.getEventType());
      Assert.assertEquals((long) (Long) sovereigntyCampaignTestData[i][5], nextCampaign.getStartTime());
      Assert.assertEquals((int) (Integer) sovereigntyCampaignTestData[i][6], nextCampaign.getDefenderID());
      Assert.assertEquals((Float) sovereigntyCampaignTestData[i][7], nextCampaign.getDefenderScore(), 0.001);
      Assert.assertEquals((Float) sovereigntyCampaignTestData[i][8], nextCampaign.getAttackersScore(), 0.001);
    }

    // Check sovereignty participant data
    for (int i = 0; i < sovereigntyCampaignTestData.length; i++) {
      int campaignID = storedCampaigns.get(i).getCampaignID();
      List<Object[]> testParts = Arrays.stream(sovereigntyCampaignParticipantTestData).filter(x -> (Integer) x[0] == campaignID).collect(Collectors.toList());
      List<SovereigntyCampaignParticipant> storedVals = storedParts.stream().filter(x -> x.getCampaignID() == campaignID).collect(Collectors.toList());
      Assert.assertEquals(testParts.size(), storedVals.size());
      for (int j = 0; j < testParts.size(); j++) {
        Object[] nextTest = testParts.get(j);
        SovereigntyCampaignParticipant nextStored = storedVals.get(j);
        Assert.assertEquals((int) (Integer) nextTest[0], nextStored.getCampaignID());
        Assert.assertEquals((int) (Integer) nextTest[1], nextStored.getAllianceID());
        Assert.assertEquals((Float) nextTest[2], nextStored.getScore(), 0.001);
      }
    }
  }

  @Test
  public void testSyncUpdate() throws Exception {
    setupMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Perform the sync
    ESISovereigntyCampaignSync sync = new ESISovereigntyCampaignSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify updated properly
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_SOV_CAMPAIGN);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_SOV_CAMPAIGN);
    long schedTime = (new DateTime(2017, 12, 21, 12, 0, 0, DateTimeZone.UTC)).getMillis();
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Populate existing
    //
    // Half the campaigns we insert are purposely not present in the server data so we can test deletion.
    // Compute the modified campaign IDs first
    int[] modifiedCampaignIDs = new int[sovereigntyCampaignTestData.length];
    for (int i = 0; i < modifiedCampaignIDs.length; i++) {
      if (i % 2 == 0)
        modifiedCampaignIDs[i] = TestBase.getUniqueRandomInteger();
      else
        modifiedCampaignIDs[i] = (int) (Integer) sovereigntyCampaignTestData[i][0];
    }

    // Populate existing campaigns
    for (int i = 0; i < sovereigntyCampaignTestData.length; i++) {
      SovereigntyCampaign newCampaign = new SovereigntyCampaign(modifiedCampaignIDs[i],
                                                                (Long) sovereigntyCampaignTestData[i][1] + 1L,
                                                                (Integer) sovereigntyCampaignTestData[i][2] + 1,
                                                                (Integer) sovereigntyCampaignTestData[i][3] + 1,
                                                                sovereigntyCampaignTestData[i][4] + "1",
                                                                (Long) sovereigntyCampaignTestData[i][5] + 1L,
                                                                (Integer) sovereigntyCampaignTestData[i][6] + 1,
                                                                (Float) sovereigntyCampaignTestData[i][7] + 1.0F,
                                                                (Float) sovereigntyCampaignTestData[i][8] + 1.0F);
      newCampaign.setup(testTime - 1);
      RefCachedData.update(newCampaign);

    }
    // Populate existing campaign participants
    for (int i = 0; i < sovereigntyCampaignParticipantTestData.length; i++) {
      // Make half the existing data have unseen corporation IDs. These items should be removed during the sync
      // since they will not be returned from the API.
      int nextCampaignID = modifiedCampaignIDs[i % modifiedCampaignIDs.length];
      SovereigntyCampaignParticipant newParticipant = new SovereigntyCampaignParticipant(nextCampaignID,
                                                                                         (Integer) sovereigntyCampaignParticipantTestData[i][1] + 1,
                                                                                         (Float) sovereigntyCampaignParticipantTestData[i][2] + 1.0F);
      newParticipant.setup(testTime - 1);
      RefCachedData.update(newParticipant);
    }

    // Perform the sync
    ESISovereigntyCampaignSync sync = new ESISovereigntyCampaignSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify old objects were evolved properly
    List<SovereigntyCampaign> oldCampaigns = AbstractESIRefSync.retrieveAll(testTime - 1, (long contid, AttributeSelector at) ->
        SovereigntyCampaign.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR,
                             AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    List<SovereigntyCampaignParticipant> oldParts = AbstractESIRefSync.retrieveAll(testTime - 1, (long contid, AttributeSelector at) ->
        SovereigntyCampaignParticipant.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(sovereigntyCampaignTestData.length, oldCampaigns.size());
    Assert.assertEquals(sovereigntyCampaignParticipantTestData.length, oldParts.size());

    // Check old campaign data
    for (int i = 0; i < sovereigntyCampaignTestData.length; i++) {
      SovereigntyCampaign nextCampaign = oldCampaigns.get(i);
      Assert.assertEquals(testTime, nextCampaign.getLifeEnd());
      Assert.assertEquals(modifiedCampaignIDs[i], nextCampaign.getCampaignID());
      Assert.assertEquals((Long) sovereigntyCampaignTestData[i][1] + 1L, nextCampaign.getStructureID());
      Assert.assertEquals((Integer) sovereigntyCampaignTestData[i][2] + 1, nextCampaign.getSystemID());
      Assert.assertEquals((Integer) sovereigntyCampaignTestData[i][3] + 1, nextCampaign.getConstellationID());
      Assert.assertEquals(sovereigntyCampaignTestData[i][4] + "1", nextCampaign.getEventType());
      Assert.assertEquals((Long) sovereigntyCampaignTestData[i][5] + 1L, nextCampaign.getStartTime());
      Assert.assertEquals((Integer) sovereigntyCampaignTestData[i][6] + 1, nextCampaign.getDefenderID());
      Assert.assertEquals((Float) sovereigntyCampaignTestData[i][7] + 1.0F, nextCampaign.getDefenderScore(), 0.001);
      Assert.assertEquals((Float) sovereigntyCampaignTestData[i][8] + 1.0F, nextCampaign.getAttackersScore(), 0.001);
    }

    // Check old campaign participant data
    for (SovereigntyCampaignParticipant nextPart : oldParts)
      Assert.assertEquals(testTime, nextPart.getLifeEnd());
    for (int i = 0; i < sovereigntyCampaignTestData.length; i++) {
      int originalCampaignID = (Integer) sovereigntyCampaignTestData[i][0];
      int modifiedCampaignID = modifiedCampaignIDs[i];
      List<Object[]> testParts = Arrays.stream(sovereigntyCampaignParticipantTestData)
                                       .filter(x -> (Integer) x[0] == originalCampaignID)
                                       .collect(Collectors.toList());
      List<SovereigntyCampaignParticipant> storedParts = oldParts.stream()
                                                                   .filter(x -> x.getCampaignID() == modifiedCampaignID)
                                                                   .collect(Collectors.toList());
      Assert.assertEquals(testParts.size(), storedParts.size());
      for (int j = 0; j < testParts.size(); j++) {
        Object[] nextTest = testParts.get(j);
        SovereigntyCampaignParticipant nextStored = storedParts.get(j);
        Assert.assertEquals((Integer) nextTest[1] + 1, nextStored.getAllianceID());
        Assert.assertEquals((Float) nextTest[2] + 1.0F, nextStored.getScore(), 0.001);
      }
    }

    // Verify updates which will also verify that all old campaigns were properly end of lifed
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_SOV_CAMPAIGN);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_SOV_CAMPAIGN);
    long schedTime = (new DateTime(2017, 12, 21, 12, 0, 0, DateTimeZone.UTC)).getMillis();
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

}
