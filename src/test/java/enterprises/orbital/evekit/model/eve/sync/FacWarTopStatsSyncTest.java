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
import enterprises.orbital.evekit.model.eve.CharacterKillStat;
import enterprises.orbital.evekit.model.eve.CharacterVictoryPointStat;
import enterprises.orbital.evekit.model.eve.CorporationKillStat;
import enterprises.orbital.evekit.model.eve.CorporationVictoryPointStat;
import enterprises.orbital.evekit.model.eve.FactionKillStat;
import enterprises.orbital.evekit.model.eve.FactionVictoryPointStat;
import enterprises.orbital.evekit.model.eve.StatAttribute;
import enterprises.orbital.evexmlapi.eve.ICharacterKillStat;
import enterprises.orbital.evexmlapi.eve.ICharacterVictoryPointStat;
import enterprises.orbital.evexmlapi.eve.ICorporationKillStat;
import enterprises.orbital.evexmlapi.eve.ICorporationVictoryPointStat;
import enterprises.orbital.evexmlapi.eve.IEveAPI;
import enterprises.orbital.evexmlapi.eve.IFacWarTopStats;
import enterprises.orbital.evexmlapi.eve.IFacWarTopSummary;
import enterprises.orbital.evexmlapi.eve.IFactionKillStat;
import enterprises.orbital.evexmlapi.eve.IFactionVictoryPointStat;

public class FacWarTopStatsSyncTest extends RefTestBase {

  // Local mocks and other objects
  long                   testDate;
  long                   prevDate;
  RefData                container;
  RefSyncTracker         tracker;
  RefSynchronizerUtil    syncUtil;
  IEveAPI                mockServer;

  static StatAttribute[] attChoices = new StatAttribute[] {
      StatAttribute.LAST_WEEK, StatAttribute.TOTAL, StatAttribute.YESTERDAY
  };
  static Object[][]      charKillStatTestData;
  static Object[][]      charVPStatTestData;
  static Object[][]      corpKillStatTestData;
  static Object[][]      corpVPStatTestData;
  static Object[][]      facKillStatTestData;
  static Object[][]      facVPStatTestData;

  static {
    // Character kill stat test data
    // 0 StatAttribute attribute
    // 1 int kills
    // 2 long characterID
    // 3 String characterName
    int size = 20 + TestBase.getRandomInt(20);
    charKillStatTestData = new Object[size][4];
    for (int i = 0; i < size; i++) {
      charKillStatTestData[i][0] = attChoices[TestBase.getRandomInt(3)];
      charKillStatTestData[i][1] = TestBase.getRandomInt();
      charKillStatTestData[i][2] = TestBase.getUniqueRandomLong();
      charKillStatTestData[i][3] = TestBase.getRandomText(50);
    }
    // Character victory point stat test data
    // 0 StatAttribute attribute
    // 1 int victoryPoints
    // 2 long characterID
    // 3 String characterName
    size = 20 + TestBase.getRandomInt(20);
    charVPStatTestData = new Object[size][4];
    for (int i = 0; i < size; i++) {
      charVPStatTestData[i][0] = attChoices[TestBase.getRandomInt(3)];
      charVPStatTestData[i][1] = TestBase.getRandomInt();
      charVPStatTestData[i][2] = TestBase.getUniqueRandomLong();
      charVPStatTestData[i][3] = TestBase.getRandomText(50);
    }
    // Corporation kill stat test data
    // 0 StatAttribute attribute
    // 1 int kills
    // 2 long corporationID
    // 3 String corporationName
    size = 20 + TestBase.getRandomInt(20);
    corpKillStatTestData = new Object[size][4];
    for (int i = 0; i < size; i++) {
      corpKillStatTestData[i][0] = attChoices[TestBase.getRandomInt(3)];
      corpKillStatTestData[i][1] = TestBase.getRandomInt();
      corpKillStatTestData[i][2] = TestBase.getUniqueRandomLong();
      corpKillStatTestData[i][3] = TestBase.getRandomText(50);
    }
    // Corporation victory point stat test data
    // 0 StatAttribute attribute
    // 1 int victoryPoints
    // 2 long corporationID
    // 3 String corporationName
    size = 20 + TestBase.getRandomInt(20);
    corpVPStatTestData = new Object[size][4];
    for (int i = 0; i < size; i++) {
      corpVPStatTestData[i][0] = attChoices[TestBase.getRandomInt(3)];
      corpVPStatTestData[i][1] = TestBase.getRandomInt();
      corpVPStatTestData[i][2] = TestBase.getUniqueRandomLong();
      corpVPStatTestData[i][3] = TestBase.getRandomText(50);
    }
    // Faction kill stat test data
    // 0 StatAttribute attribute
    // 1 int kills
    // 2 long factionID
    // 3 String factionName
    size = 20 + TestBase.getRandomInt(20);
    facKillStatTestData = new Object[size][4];
    for (int i = 0; i < size; i++) {
      facKillStatTestData[i][0] = attChoices[TestBase.getRandomInt(3)];
      facKillStatTestData[i][1] = TestBase.getRandomInt();
      facKillStatTestData[i][2] = TestBase.getUniqueRandomLong();
      facKillStatTestData[i][3] = TestBase.getRandomText(50);
    }
    // Faction victory point stat test data
    // 0 StatAttribute attribute
    // 1 int victoryPoints
    // 2 long factionID
    // 3 String factionName
    size = 20 + TestBase.getRandomInt(20);
    facVPStatTestData = new Object[size][4];
    for (int i = 0; i < size; i++) {
      facVPStatTestData[i][0] = attChoices[TestBase.getRandomInt(3)];
      facVPStatTestData[i][1] = TestBase.getRandomInt();
      facVPStatTestData[i][2] = TestBase.getUniqueRandomLong();
      facVPStatTestData[i][3] = TestBase.getRandomText(50);
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
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM CharacterKillStat").executeUpdate();
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM CharacterVictoryPointStat").executeUpdate();
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM CorporationKillStat").executeUpdate();
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM CorporationVictoryPointStat").executeUpdate();
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM FactionKillStat").executeUpdate();
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM FactionVictoryPointStat").executeUpdate();
      }
    });

    super.teardown();
  }

  protected ICharacterKillStat makeCharKillStat(
                                                Object[] data) {
    return new ICharacterKillStat() {

      @Override
      public int getKills() {
        return (Integer) data[1];
      }

      @Override
      public long getCharacterID() {
        return (Long) data[2];
      }

      @Override
      public String getCharacterName() {
        return (String) data[3];
      }

    };
  }

  protected ICharacterVictoryPointStat makeCharVPStat(
                                                      Object[] data) {
    return new ICharacterVictoryPointStat() {

      @Override
      public int getVictoryPoints() {
        return (Integer) data[1];
      }

      @Override
      public long getCharacterID() {
        return (Long) data[2];
      }

      @Override
      public String getCharacterName() {
        return (String) data[3];
      }

    };
  }

  protected ICorporationKillStat makeCorpKillStat(
                                                  Object[] data) {
    return new ICorporationKillStat() {

      @Override
      public int getKills() {
        return (Integer) data[1];
      }

      @Override
      public long getCorporationID() {
        return (Long) data[2];
      }

      @Override
      public String getCorporationName() {
        return (String) data[3];
      }

    };
  }

  protected ICorporationVictoryPointStat makeCorpVPStat(
                                                        Object[] data) {
    return new ICorporationVictoryPointStat() {

      @Override
      public int getVictoryPoints() {
        return (Integer) data[1];
      }

      @Override
      public long getCorporationID() {
        return (Long) data[2];
      }

      @Override
      public String getCorporationName() {
        return (String) data[3];
      }

    };
  }

  protected IFactionKillStat makeFactionKillStat(
                                                 Object[] data) {
    return new IFactionKillStat() {

      @Override
      public int getKills() {
        return (Integer) data[1];
      }

      @Override
      public long getFactionID() {
        return (Long) data[2];
      }

      @Override
      public String getFactionName() {
        return (String) data[3];
      }

    };
  }

  protected IFactionVictoryPointStat makeFactionVPStat(
                                                       Object[] data) {
    return new IFactionVictoryPointStat() {

      @Override
      public int getVictoryPoints() {
        return (Integer) data[1];
      }

      @Override
      public long getFactionID() {
        return (Long) data[2];
      }

      @Override
      public String getFactionName() {
        return (String) data[3];
      }

    };
  }

  // Mock up server interface
  public void setupOkMock() throws Exception {
    mockServer = EasyMock.createMock(IEveAPI.class);
    IFacWarTopStats<ICharacterKillStat, ICharacterVictoryPointStat> charStats = new IFacWarTopStats<ICharacterKillStat, ICharacterVictoryPointStat>() {

      @Override
      public Collection<ICharacterKillStat> getKillsLastWeek() {
        List<ICharacterKillStat> stats = new ArrayList<>();
        for (int i = 0; i < charKillStatTestData.length; i++) {
          if ((StatAttribute) charKillStatTestData[i][0] == StatAttribute.LAST_WEEK) {
            stats.add(makeCharKillStat(charKillStatTestData[i]));
          }
        }
        return stats;
      }

      @Override
      public Collection<ICharacterKillStat> getKillsTotal() {
        List<ICharacterKillStat> stats = new ArrayList<>();
        for (int i = 0; i < charKillStatTestData.length; i++) {
          if ((StatAttribute) charKillStatTestData[i][0] == StatAttribute.TOTAL) {
            stats.add(makeCharKillStat(charKillStatTestData[i]));
          }
        }
        return stats;
      }

      @Override
      public Collection<ICharacterKillStat> getKillsYesterday() {
        List<ICharacterKillStat> stats = new ArrayList<>();
        for (int i = 0; i < charKillStatTestData.length; i++) {
          if ((StatAttribute) charKillStatTestData[i][0] == StatAttribute.YESTERDAY) {
            stats.add(makeCharKillStat(charKillStatTestData[i]));
          }
        }
        return stats;
      }

      @Override
      public Collection<ICharacterVictoryPointStat> getVictoryPointsLastWeek() {
        List<ICharacterVictoryPointStat> stats = new ArrayList<>();
        for (int i = 0; i < charVPStatTestData.length; i++) {
          if ((StatAttribute) charVPStatTestData[i][0] == StatAttribute.LAST_WEEK) {
            stats.add(makeCharVPStat(charVPStatTestData[i]));
          }
        }
        return stats;
      }

      @Override
      public Collection<ICharacterVictoryPointStat> getVictoryPointsTotal() {
        List<ICharacterVictoryPointStat> stats = new ArrayList<>();
        for (int i = 0; i < charVPStatTestData.length; i++) {
          if ((StatAttribute) charVPStatTestData[i][0] == StatAttribute.TOTAL) {
            stats.add(makeCharVPStat(charVPStatTestData[i]));
          }
        }
        return stats;
      }

      @Override
      public Collection<ICharacterVictoryPointStat> getVictoryPointsYesterday() {
        List<ICharacterVictoryPointStat> stats = new ArrayList<>();
        for (int i = 0; i < charVPStatTestData.length; i++) {
          if ((StatAttribute) charVPStatTestData[i][0] == StatAttribute.YESTERDAY) {
            stats.add(makeCharVPStat(charVPStatTestData[i]));
          }
        }
        return stats;
      }

    };
    IFacWarTopStats<ICorporationKillStat, ICorporationVictoryPointStat> corpStats = new IFacWarTopStats<ICorporationKillStat, ICorporationVictoryPointStat>() {

      @Override
      public Collection<ICorporationKillStat> getKillsLastWeek() {
        List<ICorporationKillStat> stats = new ArrayList<>();
        for (int i = 0; i < corpKillStatTestData.length; i++) {
          if ((StatAttribute) corpKillStatTestData[i][0] == StatAttribute.LAST_WEEK) {
            stats.add(makeCorpKillStat(corpKillStatTestData[i]));
          }
        }
        return stats;
      }

      @Override
      public Collection<ICorporationKillStat> getKillsTotal() {
        List<ICorporationKillStat> stats = new ArrayList<>();
        for (int i = 0; i < corpKillStatTestData.length; i++) {
          if ((StatAttribute) corpKillStatTestData[i][0] == StatAttribute.TOTAL) {
            stats.add(makeCorpKillStat(corpKillStatTestData[i]));
          }
        }
        return stats;
      }

      @Override
      public Collection<ICorporationKillStat> getKillsYesterday() {
        List<ICorporationKillStat> stats = new ArrayList<>();
        for (int i = 0; i < corpKillStatTestData.length; i++) {
          if ((StatAttribute) corpKillStatTestData[i][0] == StatAttribute.YESTERDAY) {
            stats.add(makeCorpKillStat(corpKillStatTestData[i]));
          }
        }
        return stats;
      }

      @Override
      public Collection<ICorporationVictoryPointStat> getVictoryPointsLastWeek() {
        List<ICorporationVictoryPointStat> stats = new ArrayList<>();
        for (int i = 0; i < corpVPStatTestData.length; i++) {
          if ((StatAttribute) corpVPStatTestData[i][0] == StatAttribute.LAST_WEEK) {
            stats.add(makeCorpVPStat(corpVPStatTestData[i]));
          }
        }
        return stats;
      }

      @Override
      public Collection<ICorporationVictoryPointStat> getVictoryPointsTotal() {
        List<ICorporationVictoryPointStat> stats = new ArrayList<>();
        for (int i = 0; i < corpVPStatTestData.length; i++) {
          if ((StatAttribute) corpVPStatTestData[i][0] == StatAttribute.TOTAL) {
            stats.add(makeCorpVPStat(corpVPStatTestData[i]));
          }
        }
        return stats;
      }

      @Override
      public Collection<ICorporationVictoryPointStat> getVictoryPointsYesterday() {
        List<ICorporationVictoryPointStat> stats = new ArrayList<>();
        for (int i = 0; i < corpVPStatTestData.length; i++) {
          if ((StatAttribute) corpVPStatTestData[i][0] == StatAttribute.YESTERDAY) {
            stats.add(makeCorpVPStat(corpVPStatTestData[i]));
          }
        }
        return stats;
      }

    };
    IFacWarTopStats<IFactionKillStat, IFactionVictoryPointStat> facStats = new IFacWarTopStats<IFactionKillStat, IFactionVictoryPointStat>() {

      @Override
      public Collection<IFactionKillStat> getKillsLastWeek() {
        List<IFactionKillStat> stats = new ArrayList<>();
        for (int i = 0; i < facKillStatTestData.length; i++) {
          if ((StatAttribute) facKillStatTestData[i][0] == StatAttribute.LAST_WEEK) {
            stats.add(makeFactionKillStat(facKillStatTestData[i]));
          }
        }
        return stats;
      }

      @Override
      public Collection<IFactionKillStat> getKillsTotal() {
        List<IFactionKillStat> stats = new ArrayList<>();
        for (int i = 0; i < facKillStatTestData.length; i++) {
          if ((StatAttribute) facKillStatTestData[i][0] == StatAttribute.TOTAL) {
            stats.add(makeFactionKillStat(facKillStatTestData[i]));
          }
        }
        return stats;
      }

      @Override
      public Collection<IFactionKillStat> getKillsYesterday() {
        List<IFactionKillStat> stats = new ArrayList<>();
        for (int i = 0; i < facKillStatTestData.length; i++) {
          if ((StatAttribute) facKillStatTestData[i][0] == StatAttribute.YESTERDAY) {
            stats.add(makeFactionKillStat(facKillStatTestData[i]));
          }
        }
        return stats;
      }

      @Override
      public Collection<IFactionVictoryPointStat> getVictoryPointsLastWeek() {
        List<IFactionVictoryPointStat> stats = new ArrayList<>();
        for (int i = 0; i < facVPStatTestData.length; i++) {
          if ((StatAttribute) facVPStatTestData[i][0] == StatAttribute.LAST_WEEK) {
            stats.add(makeFactionVPStat(facVPStatTestData[i]));
          }
        }
        return stats;
      }

      @Override
      public Collection<IFactionVictoryPointStat> getVictoryPointsTotal() {
        List<IFactionVictoryPointStat> stats = new ArrayList<>();
        for (int i = 0; i < facVPStatTestData.length; i++) {
          if ((StatAttribute) facVPStatTestData[i][0] == StatAttribute.TOTAL) {
            stats.add(makeFactionVPStat(facVPStatTestData[i]));
          }
        }
        return stats;
      }

      @Override
      public Collection<IFactionVictoryPointStat> getVictoryPointsYesterday() {
        List<IFactionVictoryPointStat> stats = new ArrayList<>();
        for (int i = 0; i < facVPStatTestData.length; i++) {
          if ((StatAttribute) facVPStatTestData[i][0] == StatAttribute.YESTERDAY) {
            stats.add(makeFactionVPStat(facVPStatTestData[i]));
          }
        }
        return stats;
      }

    };
    IFacWarTopSummary summary = new IFacWarTopSummary() {

      @Override
      public IFacWarTopStats<ICharacterKillStat, ICharacterVictoryPointStat> getCharacterStats() {
        return charStats;
      }

      @Override
      public IFacWarTopStats<ICorporationKillStat, ICorporationVictoryPointStat> getCorporationStats() {
        return corpStats;
      }

      @Override
      public IFacWarTopStats<IFactionKillStat, IFactionVictoryPointStat> getFactionStats() {
        return facStats;
      }

    };

    EasyMock.expect(mockServer.requestFacWarTopStats()).andReturn(summary);
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
    SyncStatus syncOutcome = FacWarTopStatsSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<CharacterKillStat> storedCharKills = retrieveAll(new BatchRetriever<CharacterKillStat>() {

      @Override
      public List<CharacterKillStat> getNextBatch(
                                                  List<CharacterKillStat> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return CharacterKillStat.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<CharacterVictoryPointStat> storedCharVPs = retrieveAll(new BatchRetriever<CharacterVictoryPointStat>() {

      @Override
      public List<CharacterVictoryPointStat> getNextBatch(
                                                          List<CharacterVictoryPointStat> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return CharacterVictoryPointStat.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<CorporationKillStat> storedCorpKills = retrieveAll(new BatchRetriever<CorporationKillStat>() {

      @Override
      public List<CorporationKillStat> getNextBatch(
                                                    List<CorporationKillStat> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return CorporationKillStat.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<CorporationVictoryPointStat> storedCorpVPs = retrieveAll(new BatchRetriever<CorporationVictoryPointStat>() {

      @Override
      public List<CorporationVictoryPointStat> getNextBatch(
                                                            List<CorporationVictoryPointStat> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return CorporationVictoryPointStat.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<FactionKillStat> storedFactionKills = retrieveAll(new BatchRetriever<FactionKillStat>() {

      @Override
      public List<FactionKillStat> getNextBatch(
                                                List<FactionKillStat> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return FactionKillStat.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<FactionVictoryPointStat> storedFactionVPs = retrieveAll(new BatchRetriever<FactionVictoryPointStat>() {

      @Override
      public List<FactionVictoryPointStat> getNextBatch(
                                                        List<FactionVictoryPointStat> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return FactionVictoryPointStat.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(charKillStatTestData.length, storedCharKills.size());
    Assert.assertEquals(charVPStatTestData.length, storedCharVPs.size());
    Assert.assertEquals(corpKillStatTestData.length, storedCorpKills.size());
    Assert.assertEquals(corpVPStatTestData.length, storedCorpVPs.size());
    Assert.assertEquals(facKillStatTestData.length, storedFactionKills.size());
    Assert.assertEquals(facVPStatTestData.length, storedFactionVPs.size());
    // Kill stats are populated char, corp, faction by last week, total yesterday
    // Similar for VP stats. Need to sort the test data in this manner for a proper comparison
    Object[][] sortedCharKillStatTestData = new Object[charKillStatTestData.length][4];
    int k = 0;
    for (StatAttribute attr : attChoices) {
      for (int i = 0; i < charKillStatTestData.length; i++) {
        if ((StatAttribute) charKillStatTestData[i][0] == attr) System.arraycopy(charKillStatTestData[i], 0, sortedCharKillStatTestData[k++], 0, 4);
      }
    }
    Object[][] sortedCharVPStatTestData = new Object[charVPStatTestData.length][4];
    k = 0;
    for (StatAttribute attr : attChoices) {
      for (int i = 0; i < charVPStatTestData.length; i++) {
        if ((StatAttribute) charVPStatTestData[i][0] == attr) System.arraycopy(charVPStatTestData[i], 0, sortedCharVPStatTestData[k++], 0, 4);
      }
    }
    Object[][] sortedCorpKillStatTestData = new Object[corpKillStatTestData.length][4];
    k = 0;
    for (StatAttribute attr : attChoices) {
      for (int i = 0; i < corpKillStatTestData.length; i++) {
        if ((StatAttribute) corpKillStatTestData[i][0] == attr) System.arraycopy(corpKillStatTestData[i], 0, sortedCorpKillStatTestData[k++], 0, 4);
      }
    }
    Object[][] sortedCorpVPStatTestData = new Object[corpVPStatTestData.length][4];
    k = 0;
    for (StatAttribute attr : attChoices) {
      for (int i = 0; i < corpVPStatTestData.length; i++) {
        if ((StatAttribute) corpVPStatTestData[i][0] == attr) System.arraycopy(corpVPStatTestData[i], 0, sortedCorpVPStatTestData[k++], 0, 4);
      }
    }
    Object[][] sortedFactionKillStatTestData = new Object[facKillStatTestData.length][4];
    k = 0;
    for (StatAttribute attr : attChoices) {
      for (int i = 0; i < facKillStatTestData.length; i++) {
        if ((StatAttribute) facKillStatTestData[i][0] == attr) System.arraycopy(facKillStatTestData[i], 0, sortedFactionKillStatTestData[k++], 0, 4);
      }
    }
    Object[][] sortedFactionVPStatTestData = new Object[facVPStatTestData.length][4];
    k = 0;
    for (StatAttribute attr : attChoices) {
      for (int i = 0; i < facVPStatTestData.length; i++) {
        if ((StatAttribute) facVPStatTestData[i][0] == attr) System.arraycopy(facVPStatTestData[i], 0, sortedFactionVPStatTestData[k++], 0, 4);
      }
    }
    // Now run comparisons
    for (int i = 0; i < sortedCharKillStatTestData.length; i++) {
      CharacterKillStat nextStat = storedCharKills.get(i);
      Assert.assertEquals(sortedCharKillStatTestData[i][0], nextStat.getAttribute());
      Assert.assertEquals((int) (Integer) sortedCharKillStatTestData[i][1], nextStat.getKills());
      Assert.assertEquals((long) (Long) sortedCharKillStatTestData[i][2], nextStat.getCharacterID());
      Assert.assertEquals(sortedCharKillStatTestData[i][3], nextStat.getCharacterName());
    }
    for (int i = 0; i < sortedCharVPStatTestData.length; i++) {
      CharacterVictoryPointStat nextStat = storedCharVPs.get(i);
      Assert.assertEquals(sortedCharVPStatTestData[i][0], nextStat.getAttribute());
      Assert.assertEquals((int) (Integer) sortedCharVPStatTestData[i][1], nextStat.getVictoryPoints());
      Assert.assertEquals((long) (Long) sortedCharVPStatTestData[i][2], nextStat.getCharacterID());
      Assert.assertEquals(sortedCharVPStatTestData[i][3], nextStat.getCharacterName());
    }
    for (int i = 0; i < sortedCorpKillStatTestData.length; i++) {
      CorporationKillStat nextStat = storedCorpKills.get(i);
      Assert.assertEquals(sortedCorpKillStatTestData[i][0], nextStat.getAttribute());
      Assert.assertEquals((int) (Integer) sortedCorpKillStatTestData[i][1], nextStat.getKills());
      Assert.assertEquals((long) (Long) sortedCorpKillStatTestData[i][2], nextStat.getCorporationID());
      Assert.assertEquals(sortedCorpKillStatTestData[i][3], nextStat.getCorporationName());
    }
    for (int i = 0; i < sortedCorpVPStatTestData.length; i++) {
      CorporationVictoryPointStat nextStat = storedCorpVPs.get(i);
      Assert.assertEquals(sortedCorpVPStatTestData[i][0], nextStat.getAttribute());
      Assert.assertEquals((int) (Integer) sortedCorpVPStatTestData[i][1], nextStat.getVictoryPoints());
      Assert.assertEquals((long) (Long) sortedCorpVPStatTestData[i][2], nextStat.getCorporationID());
      Assert.assertEquals(sortedCorpVPStatTestData[i][3], nextStat.getCorporationName());
    }
    for (int i = 0; i < sortedFactionKillStatTestData.length; i++) {
      FactionKillStat nextStat = storedFactionKills.get(i);
      Assert.assertEquals(sortedFactionKillStatTestData[i][0], nextStat.getAttribute());
      Assert.assertEquals((int) (Integer) sortedFactionKillStatTestData[i][1], nextStat.getKills());
      Assert.assertEquals((long) (Long) sortedFactionKillStatTestData[i][2], nextStat.getFactionID());
      Assert.assertEquals(sortedFactionKillStatTestData[i][3], nextStat.getFactionName());
    }
    for (int i = 0; i < sortedFactionVPStatTestData.length; i++) {
      FactionVictoryPointStat nextStat = storedFactionVPs.get(i);
      Assert.assertEquals(sortedFactionVPStatTestData[i][0], nextStat.getAttribute());
      Assert.assertEquals((int) (Integer) sortedFactionVPStatTestData[i][1], nextStat.getVictoryPoints());
      Assert.assertEquals((long) (Long) sortedFactionVPStatTestData[i][2], nextStat.getFactionID());
      Assert.assertEquals(sortedFactionVPStatTestData[i][3], nextStat.getFactionName());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getFacWarTopStatsExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getFacWarTopStatsStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getFacWarTopStatsDetail());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    //
    // Half the existing data will have unseen character, corporation or faction IDs. These items
    // should be removed during the sync since they will not be returned from the API.
    for (int i = 0; i < charKillStatTestData.length; i++) {
      CharacterKillStat newStat = new CharacterKillStat(
          (StatAttribute) charKillStatTestData[i][0], (Integer) charKillStatTestData[i][1] + 1,
          i % 2 == 0 ? TestBase.getUniqueRandomLong() : (Long) charKillStatTestData[i][2], (String) charKillStatTestData[i][3] + "1");
      newStat.setup(testTime - 1);
      RefCachedData.updateData(newStat);
    }
    for (int i = 0; i < charVPStatTestData.length; i++) {
      CharacterVictoryPointStat newStat = new CharacterVictoryPointStat(
          (StatAttribute) charVPStatTestData[i][0], (Integer) charVPStatTestData[i][1] + 1,
          i % 2 == 0 ? TestBase.getUniqueRandomLong() : (Long) charVPStatTestData[i][2], (String) charVPStatTestData[i][3] + "1");
      newStat.setup(testTime - 1);
      RefCachedData.updateData(newStat);
    }
    for (int i = 0; i < corpKillStatTestData.length; i++) {
      CorporationKillStat newStat = new CorporationKillStat(
          (StatAttribute) corpKillStatTestData[i][0], (Integer) corpKillStatTestData[i][1] + 1,
          i % 2 == 0 ? TestBase.getUniqueRandomLong() : (Long) corpKillStatTestData[i][2], (String) corpKillStatTestData[i][3] + "1");
      newStat.setup(testTime - 1);
      RefCachedData.updateData(newStat);
    }
    for (int i = 0; i < corpVPStatTestData.length; i++) {
      CorporationVictoryPointStat newStat = new CorporationVictoryPointStat(
          (StatAttribute) corpVPStatTestData[i][0], (Integer) corpVPStatTestData[i][1] + 1,
          i % 2 == 0 ? TestBase.getUniqueRandomLong() : (Long) corpVPStatTestData[i][2], (String) corpVPStatTestData[i][3] + "1");
      newStat.setup(testTime - 1);
      RefCachedData.updateData(newStat);
    }
    for (int i = 0; i < facKillStatTestData.length; i++) {
      FactionKillStat newStat = new FactionKillStat(
          (StatAttribute) facKillStatTestData[i][0], (Integer) facKillStatTestData[i][1] + 1,
          i % 2 == 0 ? TestBase.getUniqueRandomLong() : (Long) facKillStatTestData[i][2], (String) facKillStatTestData[i][3] + "1");
      newStat.setup(testTime - 1);
      RefCachedData.updateData(newStat);
    }
    for (int i = 0; i < facVPStatTestData.length; i++) {
      FactionVictoryPointStat newStat = new FactionVictoryPointStat(
          (StatAttribute) facVPStatTestData[i][0], (Integer) facVPStatTestData[i][1] + 1,
          i % 2 == 0 ? TestBase.getUniqueRandomLong() : (Long) facVPStatTestData[i][2], (String) facVPStatTestData[i][3] + "1");
      newStat.setup(testTime - 1);
      RefCachedData.updateData(newStat);
    }

    // Perform the sync
    SyncStatus syncOutcome = FacWarTopStatsSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<CharacterKillStat> storedCharKills = retrieveAll(new BatchRetriever<CharacterKillStat>() {

      @Override
      public List<CharacterKillStat> getNextBatch(
                                                  List<CharacterKillStat> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return CharacterKillStat.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<CharacterVictoryPointStat> storedCharVPs = retrieveAll(new BatchRetriever<CharacterVictoryPointStat>() {

      @Override
      public List<CharacterVictoryPointStat> getNextBatch(
                                                          List<CharacterVictoryPointStat> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return CharacterVictoryPointStat.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<CorporationKillStat> storedCorpKills = retrieveAll(new BatchRetriever<CorporationKillStat>() {

      @Override
      public List<CorporationKillStat> getNextBatch(
                                                    List<CorporationKillStat> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return CorporationKillStat.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<CorporationVictoryPointStat> storedCorpVPs = retrieveAll(new BatchRetriever<CorporationVictoryPointStat>() {

      @Override
      public List<CorporationVictoryPointStat> getNextBatch(
                                                            List<CorporationVictoryPointStat> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return CorporationVictoryPointStat.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<FactionKillStat> storedFactionKills = retrieveAll(new BatchRetriever<FactionKillStat>() {

      @Override
      public List<FactionKillStat> getNextBatch(
                                                List<FactionKillStat> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return FactionKillStat.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<FactionVictoryPointStat> storedFactionVPs = retrieveAll(new BatchRetriever<FactionVictoryPointStat>() {

      @Override
      public List<FactionVictoryPointStat> getNextBatch(
                                                        List<FactionVictoryPointStat> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return FactionVictoryPointStat.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(charKillStatTestData.length, storedCharKills.size());
    Assert.assertEquals(charVPStatTestData.length, storedCharVPs.size());
    Assert.assertEquals(corpKillStatTestData.length, storedCorpKills.size());
    Assert.assertEquals(corpVPStatTestData.length, storedCorpVPs.size());
    Assert.assertEquals(facKillStatTestData.length, storedFactionKills.size());
    Assert.assertEquals(facVPStatTestData.length, storedFactionVPs.size());
    // Kill stats are populated char, corp, faction by last week, total yesterday
    // Similar for VP stats. Need to sort the test data in this manner for a proper comparison
    Object[][] sortedCharKillStatTestData = new Object[charKillStatTestData.length][4];
    int k = 0;
    for (StatAttribute attr : attChoices) {
      for (int i = 0; i < charKillStatTestData.length; i++) {
        if ((StatAttribute) charKillStatTestData[i][0] == attr) System.arraycopy(charKillStatTestData[i], 0, sortedCharKillStatTestData[k++], 0, 4);
      }
    }
    Object[][] sortedCharVPStatTestData = new Object[charVPStatTestData.length][4];
    k = 0;
    for (StatAttribute attr : attChoices) {
      for (int i = 0; i < charVPStatTestData.length; i++) {
        if ((StatAttribute) charVPStatTestData[i][0] == attr) System.arraycopy(charVPStatTestData[i], 0, sortedCharVPStatTestData[k++], 0, 4);
      }
    }
    Object[][] sortedCorpKillStatTestData = new Object[corpKillStatTestData.length][4];
    k = 0;
    for (StatAttribute attr : attChoices) {
      for (int i = 0; i < corpKillStatTestData.length; i++) {
        if ((StatAttribute) corpKillStatTestData[i][0] == attr) System.arraycopy(corpKillStatTestData[i], 0, sortedCorpKillStatTestData[k++], 0, 4);
      }
    }
    Object[][] sortedCorpVPStatTestData = new Object[corpVPStatTestData.length][4];
    k = 0;
    for (StatAttribute attr : attChoices) {
      for (int i = 0; i < corpVPStatTestData.length; i++) {
        if ((StatAttribute) corpVPStatTestData[i][0] == attr) System.arraycopy(corpVPStatTestData[i], 0, sortedCorpVPStatTestData[k++], 0, 4);
      }
    }
    Object[][] sortedFactionKillStatTestData = new Object[facKillStatTestData.length][4];
    k = 0;
    for (StatAttribute attr : attChoices) {
      for (int i = 0; i < facKillStatTestData.length; i++) {
        if ((StatAttribute) facKillStatTestData[i][0] == attr) System.arraycopy(facKillStatTestData[i], 0, sortedFactionKillStatTestData[k++], 0, 4);
      }
    }
    Object[][] sortedFactionVPStatTestData = new Object[facVPStatTestData.length][4];
    k = 0;
    for (StatAttribute attr : attChoices) {
      for (int i = 0; i < facVPStatTestData.length; i++) {
        if ((StatAttribute) facVPStatTestData[i][0] == attr) System.arraycopy(facVPStatTestData[i], 0, sortedFactionVPStatTestData[k++], 0, 4);
      }
    }
    // Now run comparisons
    for (int i = 0; i < sortedCharKillStatTestData.length; i++) {
      CharacterKillStat nextStat = storedCharKills.get(i);
      Assert.assertEquals(sortedCharKillStatTestData[i][0], nextStat.getAttribute());
      Assert.assertEquals((int) (Integer) sortedCharKillStatTestData[i][1], nextStat.getKills());
      Assert.assertEquals((long) (Long) sortedCharKillStatTestData[i][2], nextStat.getCharacterID());
      Assert.assertEquals(sortedCharKillStatTestData[i][3], nextStat.getCharacterName());
    }
    for (int i = 0; i < sortedCharVPStatTestData.length; i++) {
      CharacterVictoryPointStat nextStat = storedCharVPs.get(i);
      Assert.assertEquals(sortedCharVPStatTestData[i][0], nextStat.getAttribute());
      Assert.assertEquals((int) (Integer) sortedCharVPStatTestData[i][1], nextStat.getVictoryPoints());
      Assert.assertEquals((long) (Long) sortedCharVPStatTestData[i][2], nextStat.getCharacterID());
      Assert.assertEquals(sortedCharVPStatTestData[i][3], nextStat.getCharacterName());
    }
    for (int i = 0; i < sortedCorpKillStatTestData.length; i++) {
      CorporationKillStat nextStat = storedCorpKills.get(i);
      Assert.assertEquals(sortedCorpKillStatTestData[i][0], nextStat.getAttribute());
      Assert.assertEquals((int) (Integer) sortedCorpKillStatTestData[i][1], nextStat.getKills());
      Assert.assertEquals((long) (Long) sortedCorpKillStatTestData[i][2], nextStat.getCorporationID());
      Assert.assertEquals(sortedCorpKillStatTestData[i][3], nextStat.getCorporationName());
    }
    for (int i = 0; i < sortedCorpVPStatTestData.length; i++) {
      CorporationVictoryPointStat nextStat = storedCorpVPs.get(i);
      Assert.assertEquals(sortedCorpVPStatTestData[i][0], nextStat.getAttribute());
      Assert.assertEquals((int) (Integer) sortedCorpVPStatTestData[i][1], nextStat.getVictoryPoints());
      Assert.assertEquals((long) (Long) sortedCorpVPStatTestData[i][2], nextStat.getCorporationID());
      Assert.assertEquals(sortedCorpVPStatTestData[i][3], nextStat.getCorporationName());
    }
    for (int i = 0; i < sortedFactionKillStatTestData.length; i++) {
      FactionKillStat nextStat = storedFactionKills.get(i);
      Assert.assertEquals(sortedFactionKillStatTestData[i][0], nextStat.getAttribute());
      Assert.assertEquals((int) (Integer) sortedFactionKillStatTestData[i][1], nextStat.getKills());
      Assert.assertEquals((long) (Long) sortedFactionKillStatTestData[i][2], nextStat.getFactionID());
      Assert.assertEquals(sortedFactionKillStatTestData[i][3], nextStat.getFactionName());
    }
    for (int i = 0; i < sortedFactionVPStatTestData.length; i++) {
      FactionVictoryPointStat nextStat = storedFactionVPs.get(i);
      Assert.assertEquals(sortedFactionVPStatTestData[i][0], nextStat.getAttribute());
      Assert.assertEquals((int) (Integer) sortedFactionVPStatTestData[i][1], nextStat.getVictoryPoints());
      Assert.assertEquals((long) (Long) sortedFactionVPStatTestData[i][2], nextStat.getFactionID());
      Assert.assertEquals(sortedFactionVPStatTestData[i][3], nextStat.getFactionName());
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getFacWarTopStatsExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getFacWarTopStatsStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getFacWarTopStatsDetail());
  }

  // Test skips update when already updated
  @Test
  public void testSyncUpdateSkip() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < charKillStatTestData.length; i++) {
      CharacterKillStat newStat = new CharacterKillStat(
          (StatAttribute) charKillStatTestData[i][0], (Integer) charKillStatTestData[i][1] + 1, (Long) charKillStatTestData[i][2] + 1,
          (String) charKillStatTestData[i][3] + "1");
      newStat.setup(testTime - 1);
      RefCachedData.updateData(newStat);
    }
    for (int i = 0; i < charVPStatTestData.length; i++) {
      CharacterVictoryPointStat newStat = new CharacterVictoryPointStat(
          (StatAttribute) charVPStatTestData[i][0], (Integer) charVPStatTestData[i][1] + 1, (Long) charVPStatTestData[i][2] + 1,
          (String) charVPStatTestData[i][3] + "1");
      newStat.setup(testTime - 1);
      RefCachedData.updateData(newStat);
    }
    for (int i = 0; i < corpKillStatTestData.length; i++) {
      CorporationKillStat newStat = new CorporationKillStat(
          (StatAttribute) corpKillStatTestData[i][0], (Integer) corpKillStatTestData[i][1] + 1, (Long) corpKillStatTestData[i][2] + 1,
          (String) corpKillStatTestData[i][3] + "1");
      newStat.setup(testTime - 1);
      RefCachedData.updateData(newStat);
    }
    for (int i = 0; i < corpVPStatTestData.length; i++) {
      CorporationVictoryPointStat newStat = new CorporationVictoryPointStat(
          (StatAttribute) corpVPStatTestData[i][0], (Integer) corpVPStatTestData[i][1] + 1, (Long) corpVPStatTestData[i][2] + 1,
          (String) corpVPStatTestData[i][3] + "1");
      newStat.setup(testTime - 1);
      RefCachedData.updateData(newStat);
    }
    for (int i = 0; i < facKillStatTestData.length; i++) {
      FactionKillStat newStat = new FactionKillStat(
          (StatAttribute) facKillStatTestData[i][0], (Integer) facKillStatTestData[i][1] + 1, (Long) facKillStatTestData[i][2] + 1,
          (String) facKillStatTestData[i][3] + "1");
      newStat.setup(testTime - 1);
      RefCachedData.updateData(newStat);
    }
    for (int i = 0; i < facVPStatTestData.length; i++) {
      FactionVictoryPointStat newStat = new FactionVictoryPointStat(
          (StatAttribute) facVPStatTestData[i][0], (Integer) facVPStatTestData[i][1] + 1, (Long) facVPStatTestData[i][2] + 1,
          (String) facVPStatTestData[i][3] + "1");
      newStat.setup(testTime - 1);
      RefCachedData.updateData(newStat);
    }

    // Set the tracker as already updated and populate the container
    tracker.setFacWarTopStatsStatus(SyncState.UPDATED);
    tracker.setFacWarTopStatsDetail(null);
    tracker = RefSyncTracker.updateTracker(tracker);
    container.setFacWarTopStatsExpiry(prevDate);
    container = RefCachedData.updateData(container);

    // Perform the sync
    SyncStatus syncOutcome = FacWarTopStatsSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    // Skip the verify here since the calls should never be made

    // Verify unchanged
    List<CharacterKillStat> storedCharKills = retrieveAll(new BatchRetriever<CharacterKillStat>() {

      @Override
      public List<CharacterKillStat> getNextBatch(
                                                  List<CharacterKillStat> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return CharacterKillStat.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<CharacterVictoryPointStat> storedCharVPs = retrieveAll(new BatchRetriever<CharacterVictoryPointStat>() {

      @Override
      public List<CharacterVictoryPointStat> getNextBatch(
                                                          List<CharacterVictoryPointStat> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return CharacterVictoryPointStat.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<CorporationKillStat> storedCorpKills = retrieveAll(new BatchRetriever<CorporationKillStat>() {

      @Override
      public List<CorporationKillStat> getNextBatch(
                                                    List<CorporationKillStat> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return CorporationKillStat.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<CorporationVictoryPointStat> storedCorpVPs = retrieveAll(new BatchRetriever<CorporationVictoryPointStat>() {

      @Override
      public List<CorporationVictoryPointStat> getNextBatch(
                                                            List<CorporationVictoryPointStat> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return CorporationVictoryPointStat.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<FactionKillStat> storedFactionKills = retrieveAll(new BatchRetriever<FactionKillStat>() {

      @Override
      public List<FactionKillStat> getNextBatch(
                                                List<FactionKillStat> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return FactionKillStat.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<FactionVictoryPointStat> storedFactionVPs = retrieveAll(new BatchRetriever<FactionVictoryPointStat>() {

      @Override
      public List<FactionVictoryPointStat> getNextBatch(
                                                        List<FactionVictoryPointStat> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return FactionVictoryPointStat.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(charKillStatTestData.length, storedCharKills.size());
    Assert.assertEquals(charVPStatTestData.length, storedCharVPs.size());
    Assert.assertEquals(corpKillStatTestData.length, storedCorpKills.size());
    Assert.assertEquals(corpVPStatTestData.length, storedCorpVPs.size());
    Assert.assertEquals(facKillStatTestData.length, storedFactionKills.size());
    Assert.assertEquals(facVPStatTestData.length, storedFactionVPs.size());
    for (int i = 0; i < charKillStatTestData.length; i++) {
      CharacterKillStat nextStat = storedCharKills.get(i);
      Assert.assertEquals(charKillStatTestData[i][0], nextStat.getAttribute());
      Assert.assertEquals((Integer) charKillStatTestData[i][1] + 1, nextStat.getKills());
      Assert.assertEquals((Long) charKillStatTestData[i][2] + 1, nextStat.getCharacterID());
      Assert.assertEquals(charKillStatTestData[i][3] + "1", nextStat.getCharacterName());
    }
    for (int i = 0; i < charVPStatTestData.length; i++) {
      CharacterVictoryPointStat nextStat = storedCharVPs.get(i);
      Assert.assertEquals(charVPStatTestData[i][0], nextStat.getAttribute());
      Assert.assertEquals((Integer) charVPStatTestData[i][1] + 1, nextStat.getVictoryPoints());
      Assert.assertEquals((Long) charVPStatTestData[i][2] + 1, nextStat.getCharacterID());
      Assert.assertEquals(charVPStatTestData[i][3] + "1", nextStat.getCharacterName());
    }
    for (int i = 0; i < corpKillStatTestData.length; i++) {
      CorporationKillStat nextStat = storedCorpKills.get(i);
      Assert.assertEquals(corpKillStatTestData[i][0], nextStat.getAttribute());
      Assert.assertEquals((Integer) corpKillStatTestData[i][1] + 1, nextStat.getKills());
      Assert.assertEquals((Long) corpKillStatTestData[i][2] + 1, nextStat.getCorporationID());
      Assert.assertEquals(corpKillStatTestData[i][3] + "1", nextStat.getCorporationName());
    }
    for (int i = 0; i < corpVPStatTestData.length; i++) {
      CorporationVictoryPointStat nextStat = storedCorpVPs.get(i);
      Assert.assertEquals(corpVPStatTestData[i][0], nextStat.getAttribute());
      Assert.assertEquals((Integer) corpVPStatTestData[i][1] + 1, nextStat.getVictoryPoints());
      Assert.assertEquals((Long) corpVPStatTestData[i][2] + 1, nextStat.getCorporationID());
      Assert.assertEquals(corpVPStatTestData[i][3] + "1", nextStat.getCorporationName());
    }
    for (int i = 0; i < facKillStatTestData.length; i++) {
      FactionKillStat nextStat = storedFactionKills.get(i);
      Assert.assertEquals(facKillStatTestData[i][0], nextStat.getAttribute());
      Assert.assertEquals((Integer) facKillStatTestData[i][1] + 1, nextStat.getKills());
      Assert.assertEquals((Long) facKillStatTestData[i][2] + 1, nextStat.getFactionID());
      Assert.assertEquals(facKillStatTestData[i][3] + "1", nextStat.getFactionName());
    }
    for (int i = 0; i < facVPStatTestData.length; i++) {
      FactionVictoryPointStat nextStat = storedFactionVPs.get(i);
      Assert.assertEquals(facVPStatTestData[i][0], nextStat.getAttribute());
      Assert.assertEquals((Integer) facVPStatTestData[i][1] + 1, nextStat.getVictoryPoints());
      Assert.assertEquals((Long) facVPStatTestData[i][2] + 1, nextStat.getFactionID());
      Assert.assertEquals(facVPStatTestData[i][3] + "1", nextStat.getFactionName());
    }

    // Verify tracker and container unchanged
    Assert.assertEquals(prevDate, RefData.getRefData().getFacWarTopStatsExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getFacWarTopStatsStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getFacWarTopStatsDetail());
  }

}
