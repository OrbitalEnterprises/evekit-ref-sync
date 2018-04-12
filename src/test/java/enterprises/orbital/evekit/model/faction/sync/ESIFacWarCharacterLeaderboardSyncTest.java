package enterprises.orbital.evekit.model.faction.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.FactionWarfareApi;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.*;
import enterprises.orbital.evekit.TestBase;
import enterprises.orbital.evekit.account.EveKitRefDataProvider;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.faction.*;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ESIFacWarCharacterLeaderboardSyncTest extends RefTestBase {

  // Local mocks and other objects
  private ESIRefClientProvider mockServer;
  private FactionWarfareApi mockEndpoint;
  private long testTime = 1238L;

  static StatAttribute[] attChoices = new StatAttribute[]{
      StatAttribute.LAST_WEEK, StatAttribute.TOTAL, StatAttribute.YESTERDAY
  };

  private static Object[][] charLeaderboardKillsTestData;
  private static Object[][] charLeaderboardVPsTestData;

  static {
    // Faction war leaderboard kills test data
    // 0 int characterID;
    // 1 StatAttribute attribute
    // 2 int kills
    int size = 20 + TestBase.getRandomInt(20);
    charLeaderboardKillsTestData = new Object[size][3];
    for (int i = 0; i < size; i++) {
      charLeaderboardKillsTestData[i][0] = TestBase.getUniqueRandomInteger();
      charLeaderboardKillsTestData[i][1] = attChoices[TestBase.getRandomInt(attChoices.length)];
      charLeaderboardKillsTestData[i][2] = TestBase.getRandomInt();
    }

    // Faction war leaderboard victory points test data
    // 0 int characterID;
    // 1 int attribute (indexes attChoices)
    // 2 int victory points
    size = 20 + TestBase.getRandomInt(20);
    charLeaderboardVPsTestData = new Object[size][3];
    for (int i = 0; i < size; i++) {
      charLeaderboardVPsTestData[i][0] = TestBase.getUniqueRandomInteger();
      charLeaderboardVPsTestData[i][1] = attChoices[TestBase.getRandomInt(attChoices.length)];
      charLeaderboardVPsTestData[i][2] = TestBase.getRandomInt();
    }
  }

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();

    // Prepare a test sync tracker
    ESIRefEndpointSyncTracker.getOrCreateUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_CHAR_LEADERBOARD, 1234L, null);

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
                                                .createQuery("DELETE FROM CharacterKillStat ")
                                                .executeUpdate();
                           EveKitRefDataProvider.getFactory()
                                                .getEntityManager()
                                                .createQuery("DELETE FROM CharacterVictoryPointStat ")
                                                .executeUpdate();
                         });
    OrbitalProperties.setTimeGenerator(null);
    super.teardown();
  }

  // Mock up server interface
  private void setupMock() throws Exception {
    // Setup endpoint mock
    mockEndpoint = EasyMock.createMock(FactionWarfareApi.class);

    // Setup faction leaderboard call
    GetFwLeaderboardsCharactersOk charLeaderboard = new GetFwLeaderboardsCharactersOk();
    GetFwLeaderboardsCharactersKills kills = new GetFwLeaderboardsCharactersKills();
    GetFwLeaderboardsCharactersVictoryPoints vps = new GetFwLeaderboardsCharactersVictoryPoints();
    charLeaderboard.setKills(kills);
    charLeaderboard.setVictoryPoints(vps);
    // Set kills data
    kills.setActiveTotal(Arrays.stream(charLeaderboardKillsTestData)
                               .filter(x -> x[1] == StatAttribute.TOTAL)
                               .map(x -> {
                                 GetFwLeaderboardsCharactersActiveTotal stat = new GetFwLeaderboardsCharactersActiveTotal();
                                 stat.setCharacterId((Integer) x[0]);
                                 stat.setAmount((Integer) x[2]);
                                 return stat;
                               })
                               .collect(Collectors.toList()));
    kills.setLastWeek(Arrays.stream(charLeaderboardKillsTestData)
                            .filter(x -> x[1] == StatAttribute.LAST_WEEK)
                            .map(x -> {
                              GetFwLeaderboardsCharactersLastWeek stat = new GetFwLeaderboardsCharactersLastWeek();
                              stat.setCharacterId((Integer) x[0]);
                              stat.setAmount((Integer) x[2]);
                              return stat;
                            })
                            .collect(Collectors.toList()));
    kills.setYesterday(Arrays.stream(charLeaderboardKillsTestData)
                             .filter(x -> x[1] == StatAttribute.YESTERDAY)
                             .map(x -> {
                               GetFwLeaderboardsCharactersYesterday stat = new GetFwLeaderboardsCharactersYesterday();
                               stat.setCharacterId((Integer) x[0]);
                               stat.setAmount((Integer) x[2]);
                               return stat;
                             })
                             .collect(Collectors.toList()));
    // Set victory points data
    vps.setActiveTotal(Arrays.stream(charLeaderboardVPsTestData)
                             .filter(x -> x[1] == StatAttribute.TOTAL)
                             .map(x -> {
                               GetFwLeaderboardsCharactersActiveTotal1 stat = new GetFwLeaderboardsCharactersActiveTotal1();
                               stat.setCharacterId((Integer) x[0]);
                               stat.setAmount((Integer) x[2]);
                               return stat;
                             })
                             .collect(Collectors.toList()));
    vps.setLastWeek(Arrays.stream(charLeaderboardVPsTestData)
                          .filter(x -> x[1] == StatAttribute.LAST_WEEK)
                          .map(x -> {
                            GetFwLeaderboardsCharactersLastWeek1 stat = new GetFwLeaderboardsCharactersLastWeek1();
                            stat.setCharacterId((Integer) x[0]);
                            stat.setAmount((Integer) x[2]);
                            return stat;
                          })
                          .collect(Collectors.toList()));
    vps.setYesterday(Arrays.stream(charLeaderboardVPsTestData)
                           .filter(x -> x[1] == StatAttribute.YESTERDAY)
                           .map(x -> {
                             GetFwLeaderboardsCharactersYesterday1 stat = new GetFwLeaderboardsCharactersYesterday1();
                             stat.setCharacterId((Integer) x[0]);
                             stat.setAmount((Integer) x[2]);
                             return stat;
                           })
                           .collect(Collectors.toList()));

    // Setup call
    Map<String, List<String>> headers = createHeaders("Expires", "Thu, 21 Dec 2017 12:00:00 GMT");
    ApiResponse<GetFwLeaderboardsCharactersOk> charLeaderboardsResponse = new ApiResponse<>(200, headers, charLeaderboard);
    EasyMock.expect(mockEndpoint.getFwLeaderboardsCharactersWithHttpInfo(null, null, null))
            .andReturn(charLeaderboardsResponse);

    // Finally, setup client provider mock
    mockServer = EasyMock.createMock(ESIRefClientProvider.class);
    EasyMock.expect(mockServer.getFactionWarfareApi())
            .andReturn(mockEndpoint)
            .anyTimes();
  }

  private interface GetCorporation<A> {
    int getCharacterId(A src);
  }

  private interface GetAttribute<A> {
    StatAttribute getAttribute(A src);
  }

  private interface GetStat<A> {
    int getStat(A src);
  }

  private <A> void verifyMatchingData(List<Object[]> raw, List<A> stored, GetCorporation<A> gf, GetAttribute<A> ga,
                                      GetStat<A> gs) {
    Assert.assertEquals(raw.size(), stored.size());
    for (int i = 0; i < raw.size(); i++) {
      Object[] nextRaw = raw.get(i);
      A nextStored = stored.get(i);
      Assert.assertEquals((int) (Integer) nextRaw[0], gf.getCharacterId(nextStored));
      Assert.assertEquals(nextRaw[1], ga.getAttribute(nextStored));
      Assert.assertEquals((int) (Integer) nextRaw[2], gs.getStat(nextStored));
    }
  }

  private void verifyDataUpdate() throws Exception {
    // Retrieve all stored faction stats and victory points data
    List<CharacterKillStat> storedKills = AbstractESIRefSync.retrieveAll(testTime, (long contid, AttributeSelector at) ->
        CharacterKillStat.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));
    List<CharacterVictoryPointStat> storedVPs = AbstractESIRefSync.retrieveAll(testTime, (long contid, AttributeSelector at) ->
        CharacterVictoryPointStat.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(charLeaderboardKillsTestData.length, storedKills.size());
    Assert.assertEquals(charLeaderboardVPsTestData.length, storedVPs.size());

    // Check data
    verifyMatchingData(
        Arrays.stream(charLeaderboardKillsTestData)
              .filter(x -> x[1] == StatAttribute.TOTAL)
              .collect(Collectors.toList()),
        storedKills.stream()
                   .filter(x -> x.getAttribute() == StatAttribute.TOTAL)
                   .collect(Collectors.toList()),
        CharacterKillStat::getCharacterID,
        AbstractKillStat::getAttribute,
        AbstractKillStat::getKills);
    verifyMatchingData(
        Arrays.stream(charLeaderboardKillsTestData)
              .filter(x -> x[1] == StatAttribute.YESTERDAY)
              .collect(Collectors.toList()),
        storedKills.stream()
                   .filter(x -> x.getAttribute() == StatAttribute.YESTERDAY)
                   .collect(Collectors.toList()),
        CharacterKillStat::getCharacterID,
        AbstractKillStat::getAttribute,
        AbstractKillStat::getKills);
    verifyMatchingData(
        Arrays.stream(charLeaderboardKillsTestData)
              .filter(x -> x[1] == StatAttribute.LAST_WEEK)
              .collect(Collectors.toList()),
        storedKills.stream()
                   .filter(x -> x.getAttribute() == StatAttribute.LAST_WEEK)
                   .collect(Collectors.toList()),
        CharacterKillStat::getCharacterID,
        AbstractKillStat::getAttribute,
        AbstractKillStat::getKills);
    verifyMatchingData(
        Arrays.stream(charLeaderboardVPsTestData)
              .filter(x -> x[1] == StatAttribute.TOTAL)
              .collect(Collectors.toList()),
        storedVPs.stream()
                 .filter(x -> x.getAttribute() == StatAttribute.TOTAL)
                 .collect(Collectors.toList()),
        CharacterVictoryPointStat::getCharacterID,
        AbstractVictoryPointStat::getAttribute,
        AbstractVictoryPointStat::getVictoryPoints);
    verifyMatchingData(
        Arrays.stream(charLeaderboardVPsTestData)
              .filter(x -> x[1] == StatAttribute.YESTERDAY)
              .collect(Collectors.toList()),
        storedVPs.stream()
                 .filter(x -> x.getAttribute() == StatAttribute.YESTERDAY)
                 .collect(Collectors.toList()),
        CharacterVictoryPointStat::getCharacterID,
        AbstractVictoryPointStat::getAttribute,
        AbstractVictoryPointStat::getVictoryPoints);
    verifyMatchingData(
        Arrays.stream(charLeaderboardVPsTestData)
              .filter(x -> x[1] == StatAttribute.LAST_WEEK)
              .collect(Collectors.toList()),
        storedVPs.stream()
                 .filter(x -> x.getAttribute() == StatAttribute.LAST_WEEK)
                 .collect(Collectors.toList()),
        CharacterVictoryPointStat::getCharacterID,
        AbstractVictoryPointStat::getAttribute,
        AbstractVictoryPointStat::getVictoryPoints);
  }

  @Test
  public void testSyncUpdate() throws Exception {
    setupMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Perform the sync
    ESIFacWarCharacterLeaderboardSync sync = new ESIFacWarCharacterLeaderboardSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify updated properly
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_FW_CHAR_LEADERBOARD);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_CHAR_LEADERBOARD);
    long schedTime = (new DateTime(2017, 12, 21, 12, 0, 0, DateTimeZone.UTC)).getMillis();
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupMock();
    EasyMock.replay(mockServer, mockEndpoint);

    // Populate existing
    //
    // Half the stats entries we insert are purposely not present in the server data so we can test deletion.
    // Compute the modified faction IDs
    int[] modifiedKillsCharacterIDs = new int[charLeaderboardKillsTestData.length];
    for (int i = 0; i < modifiedKillsCharacterIDs.length; i++) {
      if (i % 2 == 0)
        modifiedKillsCharacterIDs[i] = TestBase.getUniqueRandomInteger();
      else
        modifiedKillsCharacterIDs[i] = (int) (Integer) charLeaderboardKillsTestData[i][0];
    }
    int[] modifiedVPsCharacterIDs = new int[charLeaderboardVPsTestData.length];
    for (int i = 0; i < modifiedVPsCharacterIDs.length; i++) {
      if (i % 2 == 0)
        modifiedVPsCharacterIDs[i] = TestBase.getUniqueRandomInteger();
      else
        modifiedVPsCharacterIDs[i] = (int) (Integer) charLeaderboardVPsTestData[i][0];
    }
    // Populate existing stats
    for (int i = 0; i < charLeaderboardKillsTestData.length; i++) {
      CharacterKillStat newStat = new CharacterKillStat((StatAttribute) charLeaderboardKillsTestData[i][1],
                                                            (Integer) charLeaderboardKillsTestData[i][2] + 1,
                                                            modifiedKillsCharacterIDs[i]);
      newStat.setup(testTime - 1);
      RefCachedData.update(newStat);
    }
    for (int i = 0; i < charLeaderboardVPsTestData.length; i++) {
      CharacterVictoryPointStat newStat = new CharacterVictoryPointStat((StatAttribute) charLeaderboardVPsTestData[i][1],
                                                                            (Integer) charLeaderboardVPsTestData[i][2] + 1,
                                                                            modifiedVPsCharacterIDs[i]);
      newStat.setup(testTime - 1);
      RefCachedData.update(newStat);
    }

    // Perform the sync
    ESIFacWarCharacterLeaderboardSync sync = new ESIFacWarCharacterLeaderboardSync();
    sync.synch(mockServer);
    EasyMock.verify(mockServer, mockEndpoint);

    // Verify old objects were evolved properly
    List<CharacterKillStat> oldKills = AbstractESIRefSync.retrieveAll(testTime - 1, (long contid, AttributeSelector at) ->
        CharacterKillStat.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));
    List<CharacterVictoryPointStat> oldVPs = AbstractESIRefSync.retrieveAll(testTime - 1, (long contid, AttributeSelector at) ->
        CharacterVictoryPointStat.accessQuery(contid, 1000, false, at, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR, AbstractESIRefSync.ANY_SELECTOR));

    // Check data matches test data
    Assert.assertEquals(charLeaderboardKillsTestData.length, oldKills.size());
    Assert.assertEquals(charLeaderboardVPsTestData.length, oldVPs.size());

    // Check old wars data
    for (int i = 0; i < charLeaderboardKillsTestData.length; i++) {
      CharacterKillStat nextStat = oldKills.get(i);
      Assert.assertEquals(testTime, nextStat.getLifeEnd());
      Assert.assertEquals(modifiedKillsCharacterIDs[i], nextStat.getCharacterID());
      Assert.assertEquals(charLeaderboardKillsTestData[i][1], nextStat.getAttribute());
      Assert.assertEquals((Integer) charLeaderboardKillsTestData[i][2] + 1, nextStat.getKills());
    }
    for (int i = 0; i < charLeaderboardVPsTestData.length; i++) {
      CharacterVictoryPointStat nextStat = oldVPs.get(i);
      Assert.assertEquals(testTime, nextStat.getLifeEnd());
      Assert.assertEquals(modifiedVPsCharacterIDs[i], nextStat.getCharacterID());
      Assert.assertEquals(charLeaderboardVPsTestData[i][1], nextStat.getAttribute());
      Assert.assertEquals((Integer) charLeaderboardVPsTestData[i][2] + 1, nextStat.getVictoryPoints());
    }

    // Verify updates which will also verify that all old alliances were properly end of lifed
    verifyDataUpdate();

    // Verify tracker was updated properly
    ESIRefEndpointSyncTracker syncTracker = ESIRefEndpointSyncTracker.getLatestFinishedTracker(ESIRefSyncEndpoint.REF_FW_CHAR_LEADERBOARD);
    Assert.assertEquals(1234L, syncTracker.getScheduled());
    Assert.assertEquals(testTime, syncTracker.getSyncStart());
    Assert.assertEquals(ESISyncState.FINISHED, syncTracker.getStatus());
    Assert.assertEquals("Updated successfully", syncTracker.getDetail());
    Assert.assertEquals(testTime, syncTracker.getSyncEnd());

    // Verify new tracker was created with next sync time
    syncTracker = ESIRefEndpointSyncTracker.getUnfinishedTracker(ESIRefSyncEndpoint.REF_FW_CHAR_LEADERBOARD);
    long schedTime = (new DateTime(2017, 12, 21, 12, 0, 0, DateTimeZone.UTC)).getMillis();
    Assert.assertEquals(schedTime, syncTracker.getScheduled());
  }

}
