package enterprises.orbital.evekit.model.calls.sync;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
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
import enterprises.orbital.evekit.model.calls.Call;
import enterprises.orbital.evekit.model.calls.CallGroup;
import enterprises.orbital.evexmlapi.IResponse;
import enterprises.orbital.evexmlapi.api.IApiAPI;
import enterprises.orbital.evexmlapi.api.ICall;
import enterprises.orbital.evexmlapi.api.ICallGroup;
import enterprises.orbital.evexmlapi.api.ICallList;

public class CallListSync extends AbstractRefSync {
  protected static final Logger log = Logger.getLogger(CallListSync.class.getName());

  @Override
  public boolean isRefreshed(
                             RefSyncTracker tracker) {
    return tracker.getCallListStatus() != SyncTracker.SyncState.NOT_PROCESSED;
  }

  @Override
  public long getExpiryTime(
                            RefData container) {
    return container.getCallListExpiry();
  }

  @Override
  public void updateStatus(
                           RefSyncTracker tracker,
                           SyncTracker.SyncState status,
                           String detail) {
    tracker.setCallListStatus(status);
    tracker.setCallListDetail(detail);
    RefSyncTracker.updateTracker(tracker);
  }

  @Override
  public void updateExpiry(
                           RefData container,
                           long expiry) {
    container.setCallListExpiry(expiry);
    RefCachedData.updateData(container);
  }

  @Override
  public boolean commit(
                        long time,
                        RefSyncTracker tracker,
                        RefData container,
                        RefCachedData item) {

    if (item instanceof Call) {
      Call api = (Call) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        Call existing = Call.get(time, api.getType(), api.getName());

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
    } else if (item instanceof CallGroup) {
      CallGroup api = (CallGroup) item;
      if (api.getLifeStart() != 0) {
        // EOL
        super.commit(time, tracker, container, api);
      } else {
        CallGroup existing = CallGroup.get(time, api.getGroupID());

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
    return ((IApiAPI) serverRequest).requestCallList();
  }

  @Override
  protected long processServerData(
                                   long time,
                                   IResponse serverRequest,
                                   Object data,
                                   List<RefCachedData> updates)
    throws IOException {
    ICallList callList = (ICallList) data;
    // Handle call groups
    Set<Long> seenGroups = new HashSet<>();
    for (ICallGroup nextGroup : callList.getCallGroups()) {
      updates.add(new CallGroup(nextGroup.getGroupID(), nextGroup.getName(), nextGroup.getDescription()));
      seenGroups.add(nextGroup.getGroupID());
    }
    // Look for groups which no longer exist and mark for deletion
    AttributeSelector ats = makeAtSelector(time);
    List<CallGroup> nextBatch = CallGroup.accessQuery(-1, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
    while (!nextBatch.isEmpty()) {
      for (CallGroup n : nextBatch) {
        if (!seenGroups.contains(n.getGroupID())) {
          n.evolve(null, time);
          updates.add(n);
        }
      }
      nextBatch = CallGroup.accessQuery(nextBatch.get(nextBatch.size() - 1).getCid(), 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
    }
    // Handle calls
    Set<String> seenCalls = new HashSet<>();
    for (ICall nextCall : callList.getCalls()) {
      updates.add(new Call(nextCall.getAccessMask(), nextCall.getType(), nextCall.getName(), nextCall.getGroupID(), nextCall.getDescription()));
      String key = nextCall.getType() + "!" + nextCall.getName();
      seenCalls.add(key);
    }
    // Look for calls which no longer exist and mark for deletion
    List<Call> nextCallBatch = Call.accessQuery(-1, 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR);
    while (!nextCallBatch.isEmpty()) {
      for (Call n : nextCallBatch) {
        String key = n.getType() + "!" + n.getName();
        if (!seenCalls.contains(key)) {
          n.evolve(null, time);
          updates.add(n);
        }
      }
      nextCallBatch = Call.accessQuery(nextCallBatch.get(nextCallBatch.size() - 1).getCid(), 1000, false, ats, ANY_SELECTOR, ANY_SELECTOR, ANY_SELECTOR,
                                       ANY_SELECTOR, ANY_SELECTOR);
    }
    return serverRequest.getCachedUntil().getTime();
  }

  private static final CallListSync syncher = new CallListSync();

  public static SyncStatus sync(
                                long time,
                                RefSynchronizerUtil syncUtil,
                                IResponse serverRequest) {
    return syncher.syncData(time, syncUtil, serverRequest, "CallList");
  }

  public static SyncStatus exclude(
                                   RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "CallList", SyncTracker.SyncState.SYNC_ERROR);
  }

  public static SyncStatus notAllowed(
                                      RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "CallList", SyncTracker.SyncState.NOT_ALLOWED);
  }

}
