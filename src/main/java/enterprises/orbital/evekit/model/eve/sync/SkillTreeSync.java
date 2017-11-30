package enterprises.orbital.evekit.model.eve.sync;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import enterprises.orbital.evekit.model.AbstractRefSync;
import enterprises.orbital.evekit.model.AttributeSelector;
import enterprises.orbital.evekit.model.RefCachedData;
import enterprises.orbital.evekit.model.RefData;
import enterprises.orbital.evekit.model.RefSyncTracker;
import enterprises.orbital.evekit.model.RefSynchronizerUtil;
import enterprises.orbital.evekit.model.RefSynchronizerUtil.SyncStatus;
import enterprises.orbital.evekit.model.SyncTracker;
import enterprises.orbital.evekit.model.eve.RequiredSkill;
import enterprises.orbital.evekit.model.eve.SkillBonus;
import enterprises.orbital.evekit.model.eve.SkillGroup;
import enterprises.orbital.evekit.model.eve.SkillMember;
import enterprises.orbital.evexmlapi.IResponse;
import enterprises.orbital.evexmlapi.eve.IBonus;
import enterprises.orbital.evexmlapi.eve.IEveAPI;
import enterprises.orbital.evexmlapi.eve.IRequiredSkill;
import enterprises.orbital.evexmlapi.eve.ISkillGroup;
import enterprises.orbital.evexmlapi.eve.ISkillMember;

public class SkillTreeSync extends AbstractRefSync {
  protected static final Logger log = Logger.getLogger(SkillTreeSync.class.getName());

  @Override
  public boolean isRefreshed(
                             RefSyncTracker tracker) {
    return tracker.getSkillTreeStatus() != SyncTracker.SyncState.NOT_PROCESSED;
  }

  @Override
  public long getExpiryTime(
                            RefData container) {
    return container.getSkillTreeExpiry();
  }

  @Override
  public void updateStatus(
                           RefSyncTracker tracker,
                           SyncTracker.SyncState status,
                           String detail) {
    tracker.setSkillTreeStatus(status);
    tracker.setSkillTreeDetail(detail);
    RefSyncTracker.updateTracker(tracker);
  }

  @Override
  public void updateExpiry(
                           RefData container,
                           long expiry) {
    container.setSkillTreeExpiry(expiry);
    RefCachedData.updateData(container);
  }

  @Override
  public boolean commit(
                        long time,
                        RefSyncTracker tracker,
                        RefData container,
                        RefCachedData item) {

    if (item instanceof RequiredSkill) {
      RequiredSkill api = (RequiredSkill) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        RequiredSkill existing = RequiredSkill.get(time, api.getParentTypeID(), api.getTypeID());

        if (existing != null) {
          if (!existing.equivalent(api)) {
            // Evolve
            existing.evolve(api, time);
            super.commit(time, tracker, container, existing);
            super.commit(time, tracker, container, api);
          }
        } else {
          // New entity
          api.setup(time);
          super.commit(time, tracker, container, api);
        }
      }
    } else if (item instanceof SkillBonus) {
      SkillBonus api = (SkillBonus) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        SkillBonus existing = SkillBonus.get(time, api.getTypeID(), api.getBonusType());

        if (existing != null) {
          if (!existing.equivalent(api)) {
            // Evolve
            existing.evolve(api, time);
            super.commit(time, tracker, container, existing);
            super.commit(time, tracker, container, api);
          }
        } else {
          // New entity
          api.setup(time);
          super.commit(time, tracker, container, api);
        }
      }
    } else if (item instanceof SkillGroup) {
      SkillGroup api = (SkillGroup) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        SkillGroup existing = SkillGroup.get(time, api.getGroupID());

        if (existing != null) {
          if (!existing.equivalent(api)) {
            // Evolve
            existing.evolve(api, time);
            super.commit(time, tracker, container, existing);
            super.commit(time, tracker, container, api);
          }
        } else {
          // New entity
          api.setup(time);
          super.commit(time, tracker, container, api);
        }
      }
    } else if (item instanceof SkillMember) {
      SkillMember api = (SkillMember) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        SkillMember existing = SkillMember.get(time, api.getTypeID());

        if (existing != null) {
          if (!existing.equivalent(api)) {
            // Evolve
            existing.evolve(api, time);
            super.commit(time, tracker, container, existing);
            super.commit(time, tracker, container, api);
          }
        } else {
          // New entity
          api.setup(time);
          super.commit(time, tracker, container, api);
        }
      }
    } else {
      // Should never happen!
      assert false;
    }

    return true;
  }

  @Override
  protected Object getServerData(
                                 IResponse serverRequest)
    throws IOException {
    return ((IEveAPI) serverRequest).requestSkillTree();
  }

  @Override
  protected long processServerData(
                                   long time,
                                   IResponse serverRequest,
                                   Object data,
                                   List<RefCachedData> updates)
    throws IOException {
    // Handle skill tree
    @SuppressWarnings("unchecked")
    Collection<ISkillGroup> skillTree = (Collection<ISkillGroup>) data;
    Set<Integer> seenGroups = new HashSet<>();
    Set<Integer> seenSkills = new HashSet<>();
    Map<Integer, Set<String>> seenBonusType = new HashMap<>();
    Map<Integer, Set<Integer>> seenRequiredSkill = new HashMap<>();
    for (ISkillGroup nextGroup : skillTree) {
      if (!seenGroups.contains(nextGroup.getGroupID())) {
        // Groups appear multiple times as they appear in sections, but we only want to add once.
        seenGroups.add(nextGroup.getGroupID());
        updates.add(new SkillGroup(nextGroup.getGroupID(), nextGroup.getGroupName()));
      }
      for (ISkillMember nextMember : nextGroup.getSkills()) {
        seenSkills.add(nextMember.getTypeID());
        updates.add(new SkillMember(
            nextMember.getGroupID(), nextMember.getTypeID(), nextMember.getDescription(), nextMember.getRank(), nextMember.getRequiredPrimaryAttribute(),
            nextMember.getRequiredSecondaryAttribute(), nextMember.getTypeName(), nextMember.isPublished()));
        for (IRequiredSkill nextRequired : nextMember.getRequiredSkills()) {
          if (!seenRequiredSkill.containsKey(nextMember.getTypeID())) seenRequiredSkill.put(nextMember.getTypeID(), new HashSet<>());
          seenRequiredSkill.get(nextMember.getTypeID()).add(nextRequired.getTypeID());
          updates.add(new RequiredSkill(nextMember.getTypeID(), nextRequired.getTypeID(), nextRequired.getLevel()));
        }
        for (IBonus nextBonus : nextMember.getBonuses()) {
          if (!seenBonusType.containsKey(nextMember.getTypeID())) seenBonusType.put(nextMember.getTypeID(), new HashSet<>());
          seenBonusType.get(nextMember.getTypeID()).add(nextBonus.getBonusType());
          updates.add(new SkillBonus(nextMember.getTypeID(), nextBonus.getBonusType(), nextBonus.getBonusValue()));
        }
      }
    }
    // Cleanup groups, skills, bonuses and required skills that no longer exist
    AttributeSelector ats = makeAtSelector(time);
    List<SkillGroup> nextGroupBatch = SkillGroup.accessQuery(-1, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
    while (!nextGroupBatch.isEmpty()) {
      for (SkillGroup n : nextGroupBatch) {
        if (!seenGroups.contains(n.getGroupID())) {
          n.evolve(null, time);
          updates.add(n);
        }
      }
      nextGroupBatch = SkillGroup.accessQuery(nextGroupBatch.get(nextGroupBatch.size() - 1).getCid(), 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR);
    }
    List<SkillMember> nextMemberBatch = SkillMember.accessQuery(-1, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                                                ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
    while (!nextMemberBatch.isEmpty()) {
      for (SkillMember n : nextMemberBatch) {
        if (!seenSkills.contains(n.getTypeID())) {
          n.evolve(null, time);
          updates.add(n);
        }
      }
      nextMemberBatch = SkillMember.accessQuery(nextMemberBatch.get(nextMemberBatch.size() - 1).getCid(), 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR,
                                                ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
    }
    List<SkillBonus> nextBonusBatch = SkillBonus.accessQuery(-1, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
    while (!nextBonusBatch.isEmpty()) {
      for (SkillBonus n : nextBonusBatch) {

        if (!seenBonusType.containsKey(n.getTypeID()) || !seenBonusType.get(n.getTypeID()).contains(n.getBonusType())) {
          n.evolve(null, time);
          updates.add(n);
        }
      }
      nextBonusBatch = SkillBonus.accessQuery(nextBonusBatch.get(nextBonusBatch.size() - 1).getCid(), 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR,
                                              ANY_SELECTOR);
    }
    List<RequiredSkill> nextRequiredBatch = RequiredSkill.accessQuery(-1, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
    while (!nextRequiredBatch.isEmpty()) {
      for (RequiredSkill n : nextRequiredBatch) {

        if (!seenRequiredSkill.containsKey(n.getParentTypeID()) || !seenRequiredSkill.get(n.getParentTypeID()).contains(n.getTypeID())) {
          n.evolve(null, time);
          updates.add(n);
        }
      }
      nextRequiredBatch = RequiredSkill.accessQuery(nextRequiredBatch.get(nextRequiredBatch.size() - 1).getCid(), 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR,
                                                    ANY_SELECTOR);
    }
    // Return cache time
    return serverRequest.getCachedUntil().getTime();
  }

  private static final SkillTreeSync syncher = new SkillTreeSync();

  public static SyncStatus sync(
                                long time,
                                RefSynchronizerUtil syncUtil,
                                IResponse serverRequest) {
    return syncher.syncData(time, syncUtil, serverRequest, "SkillTree");
  }

  public static SyncStatus exclude(
                                   RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "SkillTree", SyncTracker.SyncState.SYNC_ERROR);
  }

  public static SyncStatus notAllowed(
                                      RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "SkillTree", SyncTracker.SyncState.NOT_ALLOWED);
  }

}
