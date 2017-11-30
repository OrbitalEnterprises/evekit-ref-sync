package enterprises.orbital.evekit.model.server.sync;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import enterprises.orbital.evekit.model.AbstractRefSync;
import enterprises.orbital.evekit.model.RefCachedData;
import enterprises.orbital.evekit.model.RefData;
import enterprises.orbital.evekit.model.RefSyncTracker;
import enterprises.orbital.evekit.model.RefSynchronizerUtil;
import enterprises.orbital.evekit.model.RefSynchronizerUtil.SyncStatus;
import enterprises.orbital.evekit.model.SyncTracker;
import enterprises.orbital.evekit.model.server.ServerStatus;
import enterprises.orbital.evexmlapi.IResponse;
import enterprises.orbital.evexmlapi.svr.IServerAPI;
import enterprises.orbital.evexmlapi.svr.IServerStatus;

public class ServerStatusSync extends AbstractRefSync {
  protected static final Logger log = Logger.getLogger(ServerStatusSync.class.getName());

  @Override
  public boolean isRefreshed(
                             RefSyncTracker tracker) {
    return tracker.getServerStatusStatus() != SyncTracker.SyncState.NOT_PROCESSED;
  }

  @Override
  public long getExpiryTime(
                            RefData container) {
    return container.getServerStatusExpiry();
  }

  @Override
  public void updateStatus(
                           RefSyncTracker tracker,
                           SyncTracker.SyncState status,
                           String detail) {
    tracker.setServerStatusStatus(status);
    tracker.setServerStatusDetail(detail);
    RefSyncTracker.updateTracker(tracker);
  }

  @Override
  public void updateExpiry(
                           RefData container,
                           long expiry) {
    container.setServerStatusExpiry(expiry);
    RefCachedData.updateData(container);
  }

  @Override
  public boolean commit(
                        long time,
                        RefSyncTracker tracker,
                        RefData container,
                        RefCachedData item) {
    assert item instanceof ServerStatus;

    ServerStatus api = (ServerStatus) item;
    ServerStatus existing = ServerStatus.get(time);

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

    return true;
  }

  @Override
  protected Object getServerData(
                                 IResponse serverRequest)
    throws IOException {
    return ((IServerAPI) serverRequest).requestServerStatus();
  }

  @Override
  protected long processServerData(
                                   long time,
                                   IResponse serverRequest,
                                   Object data,
                                   List<RefCachedData> updates)
    throws IOException {
    IServerStatus status = (IServerStatus) data;
    updates.add(new ServerStatus(status.getOnlinePlayers(), status.isServerOpen()));
    return serverRequest.getCachedUntil().getTime();
  }

  private static final ServerStatusSync syncher = new ServerStatusSync();

  public static SyncStatus sync(
                                long time,
                                RefSynchronizerUtil syncUtil,
                                IResponse serverRequest) {
    return syncher.syncData(time, syncUtil, serverRequest, "ServerStatus");
  }

  public static SyncStatus exclude(
                                   RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "ServerStatus", SyncTracker.SyncState.SYNC_ERROR);
  }

  public static SyncStatus notAllowed(
                                      RefSynchronizerUtil syncUtil) {
    return syncher.excludeState(syncUtil, "ServerStatus", SyncTracker.SyncState.NOT_ALLOWED);
  }

}
