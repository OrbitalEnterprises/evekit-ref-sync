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
import enterprises.orbital.evekit.model.eve.RequiredSkill;
import enterprises.orbital.evekit.model.eve.SkillBonus;
import enterprises.orbital.evekit.model.eve.SkillGroup;
import enterprises.orbital.evekit.model.eve.SkillMember;
import enterprises.orbital.evexmlapi.eve.IBonus;
import enterprises.orbital.evexmlapi.eve.IEveAPI;
import enterprises.orbital.evexmlapi.eve.IRequiredSkill;
import enterprises.orbital.evexmlapi.eve.ISkillGroup;
import enterprises.orbital.evexmlapi.eve.ISkillMember;

public class SkillTreeSyncTest extends RefTestBase {

  // Local mocks and other objects
  long                testDate;
  long                prevDate;
  RefData             container;
  RefSyncTracker      tracker;
  RefSynchronizerUtil syncUtil;
  IEveAPI             mockServer;

  static Object[][]   skillGroupTestData;

  static {
    // Skill group test data
    // 0 int group ID
    // 1 String group Name
    // 2 array of skill members
    //
    // Skill member test data (element 2 of skill group entry)
    // 0 String description
    // 1 int group ID
    // 2 int rank
    // 3 String primaryAttribute
    // 4 String secondary Attribute
    // 5 int typeID
    // 6 String typeName
    // 7 boolean published
    // 8 array of required skills
    // 9 array of bonuses
    //
    // Required skill test data (element 8 of skill member)
    // 0 int level
    // 1 int typeID
    //
    // Bonus test data (element 9 of skill member)
    // 0 String bonusType
    // 1 String bonusValue
    int size = 20 + TestBase.getRandomInt(20);
    skillGroupTestData = new Object[size][3];
    for (int i = 0; i < size; i++) {
      skillGroupTestData[i][0] = TestBase.getUniqueRandomInteger();
      skillGroupTestData[i][1] = TestBase.getRandomText(50);
      int memberSize = 20 + TestBase.getRandomInt(20);
      Object[][] memberTestData = new Object[memberSize][10];
      skillGroupTestData[i][2] = memberTestData;
      for (int j = 0; j < memberSize; j++) {
        memberTestData[j][0] = TestBase.getRandomText(50);
        memberTestData[j][1] = skillGroupTestData[i][0];
        memberTestData[j][2] = TestBase.getRandomInt();
        memberTestData[j][3] = TestBase.getRandomText(50);
        memberTestData[j][4] = TestBase.getRandomText(50);
        memberTestData[j][5] = TestBase.getUniqueRandomInteger();
        memberTestData[j][6] = TestBase.getRandomText(50);
        memberTestData[j][7] = TestBase.getRandomBoolean();
        int reqSkillSize = 5 + TestBase.getRandomInt(5);
        int bonusSize = 5 + TestBase.getRandomInt(5);
        Object[][] reqSkillTestData = new Object[reqSkillSize][2];
        Object[][] bonusTestData = new Object[bonusSize][2];
        memberTestData[j][8] = reqSkillTestData;
        memberTestData[j][9] = bonusTestData;
        for (int k = 0; k < reqSkillSize; k++) {
          reqSkillTestData[k][0] = TestBase.getRandomInt();
          reqSkillTestData[k][1] = TestBase.getUniqueRandomInteger();
        }
        for (int k = 0; k < bonusSize; k++) {
          bonusTestData[k][0] = TestBase.getRandomText(50) + String.valueOf(memberTestData[j][5]);
          bonusTestData[k][1] = TestBase.getRandomText(50);
        }
      }
      // for (ISkillGroup nextGroup : skillTree) {
      // seenGroups.add(nextGroup.getGroupID());
      // updates.add(new SkillGroup(nextGroup.getGroupID(), nextGroup.getGroupName()));
      // for (ISkillMember nextMember : nextGroup.getSkills()) {
      // seenSkills.add(nextMember.getTypeID());
      // updates.add(new SkillMember(
      // nextMember.getDescription(), nextMember.getGroupID(), nextMember.getRequiredPrimaryAttribute(), nextMember.getRequiredSecondaryAttribute(),
      // nextMember.getTypeID(), nextMember.getTypeName(), nextMember.isPublished()));
      // for (IRequiredSkill nextRequired : nextMember.getRequiredSkills()) {
      // if (!seenRequiredSkill.containsKey(nextMember.getTypeID())) seenRequiredSkill.put(nextMember.getTypeID(), new HashSet<>());
      // seenRequiredSkill.get(nextMember.getTypeID()).add(nextRequired.getTypeID());
      // updates.add(new RequiredSkill(nextMember.getTypeID(), nextRequired.getTypeID(), nextRequired.getLevel()));
      // }
      // for (IBonus nextBonus : nextMember.getBonuses()) {
      // if (!seenBonusType.containsKey(nextMember.getTypeID())) seenBonusType.put(nextMember.getTypeID(), new HashSet<>());
      // seenBonusType.get(nextMember.getTypeID()).add(nextBonus.getBonusType());
      // updates.add(new SkillBonus(nextMember.getTypeID(), nextBonus.getBonusType(), nextBonus.getBonusValue()));
      // }
      // }
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
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM RequiredSkill").executeUpdate();
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM SkillBonus").executeUpdate();
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM SkillGroup").executeUpdate();
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM SkillMember").executeUpdate();
      }
    });

    super.teardown();
  }

  // Mock up server interface
  public void setupOkMock() throws Exception {
    mockServer = EasyMock.createMock(IEveAPI.class);
    Collection<ISkillGroup> groups = new ArrayList<>();
    for (int i = 0; i < skillGroupTestData.length; i++) {
      Object[] data = skillGroupTestData[i];
      groups.add(new ISkillGroup() {

        @Override
        public int getGroupID() {
          return (Integer) data[0];
        }

        @Override
        public String getGroupName() {
          return (String) data[1];
        }

        @Override
        public Collection<ISkillMember> getSkills() {
          Object[][] memberData = (Object[][]) data[2];
          Collection<ISkillMember> members = new ArrayList<>();
          for (int j = 0; j < memberData.length; j++) {
            Object[] instanceData = memberData[j];
            members.add(new ISkillMember() {

              @Override
              public String getDescription() {
                return (String) instanceData[0];
              }

              @Override
              public int getGroupID() {
                return (Integer) instanceData[1];
              }

              @Override
              public int getRank() {
                return (Integer) instanceData[2];
              }

              @Override
              public Collection<IRequiredSkill> getRequiredSkills() {
                Object[][] reqData = (Object[][]) instanceData[8];
                Collection<IRequiredSkill> reqs = new ArrayList<>();
                for (int k = 0; k < reqData.length; k++) {
                  Object[] reqInstance = reqData[k];
                  reqs.add(new IRequiredSkill() {

                    @Override
                    public int getLevel() {
                      return (Integer) reqInstance[0];
                    }

                    @Override
                    public int getTypeID() {
                      return (Integer) reqInstance[1];
                    }

                  });
                }
                return reqs;
              }

              @Override
              public Collection<IBonus> getBonuses() {
                Object[][] bonusData = (Object[][]) instanceData[9];
                Collection<IBonus> reqs = new ArrayList<>();
                for (int k = 0; k < bonusData.length; k++) {
                  Object[] bonusInstance = bonusData[k];
                  reqs.add(new IBonus() {

                    @Override
                    public String getBonusType() {
                      return (String) bonusInstance[0];
                    }

                    @Override
                    public String getBonusValue() {
                      return (String) bonusInstance[1];
                    }

                  });
                }
                return reqs;
              }

              @Override
              public String getRequiredPrimaryAttribute() {
                return (String) instanceData[3];
              }

              @Override
              public String getRequiredSecondaryAttribute() {
                return (String) instanceData[4];
              }

              @Override
              public int getTypeID() {
                return (Integer) instanceData[5];
              }

              @Override
              public String getTypeName() {
                return (String) instanceData[6];
              }

              @Override
              public boolean isPublished() {
                return (Boolean) instanceData[7];
              }

            });
          }
          return members;
        }

      });
    }

    EasyMock.expect(mockServer.requestSkillTree()).andReturn(groups);
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
    SyncStatus syncOutcome = SkillTreeSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<SkillGroup> storedSkillGroups = retrieveAll(new BatchRetriever<SkillGroup>() {

      @Override
      public List<SkillGroup> getNextBatch(
                                           List<SkillGroup> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return SkillGroup.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<SkillMember> storedSkillMembers = retrieveAll(new BatchRetriever<SkillMember>() {

      @Override
      public List<SkillMember> getNextBatch(
                                            List<SkillMember> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return SkillMember.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                       ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<RequiredSkill> storedRequiredSkills = retrieveAll(new BatchRetriever<RequiredSkill>() {

      @Override
      public List<RequiredSkill> getNextBatch(
                                              List<RequiredSkill> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return RequiredSkill.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<SkillBonus> storedSkillBonuses = retrieveAll(new BatchRetriever<SkillBonus>() {

      @Override
      public List<SkillBonus> getNextBatch(
                                           List<SkillBonus> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return SkillBonus.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(skillGroupTestData.length, storedSkillGroups.size());
    // Now run comparisons. We run through each type in order they were added for proper comparison.
    for (int i = 0, j = 0, k = 0, m = 0; i < skillGroupTestData.length; i++) {
      SkillGroup nextGroup = storedSkillGroups.get(i);
      Assert.assertEquals((int) (Integer) skillGroupTestData[i][0], nextGroup.getGroupID());
      Assert.assertEquals(skillGroupTestData[i][1], nextGroup.getGroupName());
      Object[][] memberData = (Object[][]) skillGroupTestData[i][2];
      for (int ii = 0; ii < memberData.length; ii++, j++) {
        SkillMember nextMember = storedSkillMembers.get(j);
        Assert.assertEquals(memberData[ii][0], nextMember.getDescription());
        Assert.assertEquals((int) (Integer) memberData[ii][1], nextMember.getGroupID());
        Assert.assertEquals((int) (Integer) memberData[ii][2], nextMember.getRank());
        Assert.assertEquals(memberData[ii][3], nextMember.getRequiredPrimaryAttribute());
        Assert.assertEquals(memberData[ii][4], nextMember.getRequiredSecondaryAttribute());
        Assert.assertEquals((int) (Integer) memberData[ii][5], nextMember.getTypeID());
        Assert.assertEquals(memberData[ii][6], nextMember.getTypeName());
        Assert.assertEquals((boolean) (Boolean) memberData[ii][7], nextMember.isPublished());
        Object[][] reqData = (Object[][]) memberData[ii][8];
        for (int iii = 0; iii < reqData.length; iii++, k++) {
          RequiredSkill nextRequired = storedRequiredSkills.get(k);
          Assert.assertEquals((int) (Integer) memberData[ii][5], nextRequired.getParentTypeID());
          Assert.assertEquals((int) (Integer) reqData[iii][0], nextRequired.getLevel());
          Assert.assertEquals((int) (Integer) reqData[iii][1], nextRequired.getTypeID());
        }
        Object[][] bonusData = (Object[][]) memberData[ii][9];
        for (int iii = 0; iii < bonusData.length; iii++, m++) {
          SkillBonus nextBonus = storedSkillBonuses.get(m);
          Assert.assertEquals((int) (Integer) memberData[ii][5], nextBonus.getTypeID());
          Assert.assertEquals(bonusData[iii][0], nextBonus.getBonusType());
          Assert.assertEquals(bonusData[iii][1], nextBonus.getBonusValue());
        }
      }
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getSkillTreeExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getSkillTreeStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getSkillTreeDetail());
  }

  @Test
  public void testSyncUpdateExisting() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    //
    // Half the existing data will have unseen group or type IDs. These items
    // should be removed during the sync since they will not be returned from the API.
    //
    for (int i = 0; i < skillGroupTestData.length; i++) {
      SkillGroup newGroup = new SkillGroup(
          i % 2 == 0 ? TestBase.getUniqueRandomInteger() : (Integer) skillGroupTestData[i][0], (String) skillGroupTestData[i][1] + "1");
      newGroup.setup(testTime - 1);
      RefCachedData.updateData(newGroup);
      Object[][] memberData = (Object[][]) skillGroupTestData[i][2];
      for (int j = 0; j < memberData.length; j++) {
        SkillMember newMember = new SkillMember(
            (Integer) memberData[j][1] + 1, j % 2 == 0 ? TestBase.getUniqueRandomInteger() : (Integer) memberData[j][5], (String) memberData[j][0] + "1",
            (Integer) memberData[j][2] + 1, (String) memberData[j][3] + "1", (String) memberData[j][4] + "1", (String) memberData[j][6] + "1",
            (Boolean) memberData[j][7]);
        newMember.setup(testTime - 1);
        RefCachedData.updateData(newMember);
        Object[][] reqData = (Object[][]) memberData[j][8];
        Object[][] bonusData = (Object[][]) memberData[j][9];
        for (int k = 0; k < reqData.length; k++) {
          RequiredSkill newReq = new RequiredSkill(
              newMember.getTypeID(), k % 2 == 0 ? TestBase.getUniqueRandomInteger() : (Integer) reqData[k][1], (Integer) reqData[k][0] + 1);
          newReq.setup(testTime - 1);
          RefCachedData.updateData(newReq);
        }
        for (int k = 0; k < bonusData.length; k++) {
          SkillBonus newBonus = new SkillBonus(newMember.getTypeID(), (String) bonusData[k][0], (String) bonusData[k][1] + "1");
          newBonus.setup(testTime - 1);
          RefCachedData.updateData(newBonus);
        }
      }
    }

    // Perform the sync
    SyncStatus syncOutcome = SkillTreeSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    EasyMock.verify(mockServer);

    // Verify updated properly
    List<SkillGroup> storedSkillGroups = retrieveAll(new BatchRetriever<SkillGroup>() {

      @Override
      public List<SkillGroup> getNextBatch(
                                           List<SkillGroup> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return SkillGroup.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<SkillMember> storedSkillMembers = retrieveAll(new BatchRetriever<SkillMember>() {

      @Override
      public List<SkillMember> getNextBatch(
                                            List<SkillMember> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return SkillMember.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                       ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<RequiredSkill> storedRequiredSkills = retrieveAll(new BatchRetriever<RequiredSkill>() {

      @Override
      public List<RequiredSkill> getNextBatch(
                                              List<RequiredSkill> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return RequiredSkill.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<SkillBonus> storedSkillBonuses = retrieveAll(new BatchRetriever<SkillBonus>() {

      @Override
      public List<SkillBonus> getNextBatch(
                                           List<SkillBonus> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return SkillBonus.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(skillGroupTestData.length, storedSkillGroups.size());
    // Now run comparisons. We run through each type in order they were added for proper comparison.
    for (int i = 0, j = 0, k = 0, m = 0; i < skillGroupTestData.length; i++) {
      SkillGroup nextGroup = storedSkillGroups.get(i);
      Assert.assertEquals((int) (Integer) skillGroupTestData[i][0], nextGroup.getGroupID());
      Assert.assertEquals(skillGroupTestData[i][1], nextGroup.getGroupName());
      Object[][] memberData = (Object[][]) skillGroupTestData[i][2];
      for (int ii = 0; ii < memberData.length; ii++, j++) {
        SkillMember nextMember = storedSkillMembers.get(j);
        Assert.assertEquals(memberData[ii][0], nextMember.getDescription());
        Assert.assertEquals((int) (Integer) memberData[ii][1], nextMember.getGroupID());
        Assert.assertEquals((int) (Integer) memberData[ii][2], nextMember.getRank());
        Assert.assertEquals(memberData[ii][3], nextMember.getRequiredPrimaryAttribute());
        Assert.assertEquals(memberData[ii][4], nextMember.getRequiredSecondaryAttribute());
        Assert.assertEquals((int) (Integer) memberData[ii][5], nextMember.getTypeID());
        Assert.assertEquals(memberData[ii][6], nextMember.getTypeName());
        Assert.assertEquals((boolean) (Boolean) memberData[ii][7], nextMember.isPublished());
        Object[][] reqData = (Object[][]) memberData[ii][8];
        for (int iii = 0; iii < reqData.length; iii++, k++) {
          RequiredSkill nextRequired = storedRequiredSkills.get(k);
          Assert.assertEquals((int) (Integer) memberData[ii][5], nextRequired.getParentTypeID());
          Assert.assertEquals((int) (Integer) reqData[iii][0], nextRequired.getLevel());
          Assert.assertEquals((int) (Integer) reqData[iii][1], nextRequired.getTypeID());
        }
        Object[][] bonusData = (Object[][]) memberData[ii][9];
        for (int iii = 0; iii < bonusData.length; iii++, m++) {
          SkillBonus nextBonus = storedSkillBonuses.get(m);
          Assert.assertEquals((int) (Integer) memberData[ii][5], nextBonus.getTypeID());
          Assert.assertEquals(bonusData[iii][0], nextBonus.getBonusType());
          Assert.assertEquals(bonusData[iii][1], nextBonus.getBonusValue());
        }
      }
    }

    // Verify tracker and container were updated properly
    Assert.assertEquals(testDate, RefData.getRefData().getSkillTreeExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getSkillTreeStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getSkillTreeDetail());
  }

  // Test skips update when already updated
  @Test
  public void testSyncUpdateSkip() throws Exception {
    setupOkMock();
    EasyMock.replay(mockServer);
    long testTime = 1234L;
    AttributeSelector ats = makeAtSelector(testTime);

    // Populate existing
    for (int i = 0; i < skillGroupTestData.length; i++) {
      SkillGroup newGroup = new SkillGroup((Integer) skillGroupTestData[i][0] + 1, (String) skillGroupTestData[i][1] + "1");
      newGroup.setup(testTime - 1);
      RefCachedData.updateData(newGroup);
      Object[][] memberData = (Object[][]) skillGroupTestData[i][2];
      for (int j = 0; j < memberData.length; j++) {
        SkillMember newMember = new SkillMember(
            (Integer) memberData[j][1] + 1, (Integer) memberData[j][5] + 1, (String) memberData[j][0] + "1", (Integer) memberData[j][2] + 1,
            (String) memberData[j][3] + "1", (String) memberData[j][4] + "1", (String) memberData[j][6] + "1", (Boolean) memberData[j][7]);
        newMember.setup(testTime - 1);
        RefCachedData.updateData(newMember);
        Object[][] reqData = (Object[][]) memberData[j][8];
        Object[][] bonusData = (Object[][]) memberData[j][9];
        for (int k = 0; k < reqData.length; k++) {
          RequiredSkill newReq = new RequiredSkill(newMember.getTypeID(), (Integer) reqData[k][1] + 1, (Integer) reqData[k][0] + 1);
          newReq.setup(testTime - 1);
          RefCachedData.updateData(newReq);
        }
        for (int k = 0; k < bonusData.length; k++) {
          SkillBonus newBonus = new SkillBonus(newMember.getTypeID(), (String) bonusData[k][0], (String) bonusData[k][1] + "1");
          newBonus.setup(testTime - 1);
          RefCachedData.updateData(newBonus);
        }
      }
    }

    // Set the tracker as already updated and populate the container
    tracker.setSkillTreeStatus(SyncState.UPDATED);
    tracker.setSkillTreeDetail(null);
    tracker = RefSyncTracker.updateTracker(tracker);
    container.setSkillTreeExpiry(prevDate);
    container = RefCachedData.updateData(container);

    // Perform the sync
    SyncStatus syncOutcome = SkillTreeSync.sync(testTime, syncUtil, mockServer);
    Assert.assertEquals(SyncStatus.DONE, syncOutcome);
    // Skip the verify here since the calls should never be made

    // Verify unchanged
    List<SkillGroup> storedSkillGroups = retrieveAll(new BatchRetriever<SkillGroup>() {

      @Override
      public List<SkillGroup> getNextBatch(
                                           List<SkillGroup> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return SkillGroup.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<SkillMember> storedSkillMembers = retrieveAll(new BatchRetriever<SkillMember>() {

      @Override
      public List<SkillMember> getNextBatch(
                                            List<SkillMember> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return SkillMember.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                       ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<RequiredSkill> storedRequiredSkills = retrieveAll(new BatchRetriever<RequiredSkill>() {

      @Override
      public List<RequiredSkill> getNextBatch(
                                              List<RequiredSkill> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return RequiredSkill.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    List<SkillBonus> storedSkillBonuses = retrieveAll(new BatchRetriever<SkillBonus>() {

      @Override
      public List<SkillBonus> getNextBatch(
                                           List<SkillBonus> lastBatch) {
        long contid = lastBatch == null || lastBatch.isEmpty() ? -1 : lastBatch.get(lastBatch.size() - 1).getCid();
        return SkillBonus.accessQuery(contid, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
      }

    });
    Assert.assertEquals(skillGroupTestData.length, storedSkillGroups.size());
    // Now run comparisons. We run through each type in order they were added for proper comparison.
    for (int i = 0, j = 0, k = 0, m = 0; i < skillGroupTestData.length; i++) {
      SkillGroup nextGroup = storedSkillGroups.get(i);
      Assert.assertEquals((Integer) skillGroupTestData[i][0] + 1, nextGroup.getGroupID());
      Assert.assertEquals(skillGroupTestData[i][1] + "1", nextGroup.getGroupName());
      Object[][] memberData = (Object[][]) skillGroupTestData[i][2];
      for (int ii = 0; ii < memberData.length; ii++, j++) {
        SkillMember nextMember = storedSkillMembers.get(j);
        Assert.assertEquals(memberData[ii][0] + "1", nextMember.getDescription());
        Assert.assertEquals((Integer) memberData[ii][1] + 1, nextMember.getGroupID());
        Assert.assertEquals((Integer) memberData[ii][2] + 1, nextMember.getRank());
        Assert.assertEquals(memberData[ii][3] + "1", nextMember.getRequiredPrimaryAttribute());
        Assert.assertEquals(memberData[ii][4] + "1", nextMember.getRequiredSecondaryAttribute());
        Assert.assertEquals((Integer) memberData[ii][5] + 1, nextMember.getTypeID());
        Assert.assertEquals(memberData[ii][6] + "1", nextMember.getTypeName());
        Assert.assertEquals((boolean) (Boolean) memberData[ii][7], nextMember.isPublished());
        Object[][] reqData = (Object[][]) memberData[ii][8];
        for (int iii = 0; iii < reqData.length; iii++, k++) {
          RequiredSkill nextRequired = storedRequiredSkills.get(k);
          Assert.assertEquals((Integer) memberData[ii][5] + 1, nextRequired.getParentTypeID());
          Assert.assertEquals((Integer) reqData[iii][0] + 1, nextRequired.getLevel());
          Assert.assertEquals((Integer) reqData[iii][1] + 1, nextRequired.getTypeID());
        }
        Object[][] bonusData = (Object[][]) memberData[ii][9];
        for (int iii = 0; iii < bonusData.length; iii++, m++) {
          SkillBonus nextBonus = storedSkillBonuses.get(m);
          Assert.assertEquals((Integer) memberData[ii][5] + 1, nextBonus.getTypeID());
          Assert.assertEquals(bonusData[iii][0], nextBonus.getBonusType());
          Assert.assertEquals(bonusData[iii][1] + "1", nextBonus.getBonusValue());
        }
      }
    }

    // Verify tracker and container unchanged
    Assert.assertEquals(prevDate, RefData.getRefData().getSkillTreeExpiry());
    Assert.assertEquals(SyncState.UPDATED, RefSyncTracker.getUnfinishedTracker().getSkillTreeStatus());
    Assert.assertNull(RefSyncTracker.getUnfinishedTracker().getSkillTreeDetail());
  }

}
