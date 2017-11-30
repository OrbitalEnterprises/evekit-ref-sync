package enterprises.orbital.evekit.model;

public interface RefSynchronizationHandler {

  /**
   * Return the currently active sync tracker, or null if there is no active sync tracker.
   * 
   * @return the currently active and unfinished sync tracker, or null if there is no active sync tracker.
   */
  public RefSyncTracker getCurrentTracker();

  /**
   * Return the existing container, or null if no container exists.
   * 
   * @return the existing container, or null if no container exists.
   */
  public RefData getExistingContainer();

  public boolean isRefreshed(
                             RefSyncTracker tracker);

  public void updateStatus(
                           RefSyncTracker tracker,
                           SyncTracker.SyncState status,
                           String detail);

  public boolean prereqSatisfied(
                                 RefSyncTracker tracker);

  public void updateExpiry(
                           RefData container,
                           long expiry);

  public long getExpiryTime(
                            RefData container);

  public boolean commit(
                        long time,
                        RefSyncTracker tracker,
                        RefData container,
                        RefCachedData item);

}
