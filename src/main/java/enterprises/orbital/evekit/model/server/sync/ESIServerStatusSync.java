package enterprises.orbital.evekit.model.server.sync;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.eve.esi.client.api.StatusApi;
import enterprises.orbital.eve.esi.client.invoker.ApiException;
import enterprises.orbital.eve.esi.client.invoker.ApiResponse;
import enterprises.orbital.eve.esi.client.model.GetStatusOk;
import enterprises.orbital.evekit.model.*;
import enterprises.orbital.evekit.model.server.ServerStatus;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

public class ESIServerStatusSync extends AbstractESIRefSync<GetStatusOk> {
  protected static final Logger log = Logger.getLogger(ESIServerStatusSync.class.getName());

  @Override
  public ESIRefSyncEndpoint endpoint() {
    return ESIRefSyncEndpoint.REF_SERVER_STATUS;
  }

  @Override
  protected void commit(long time,
                        RefCachedData item) throws IOException {
    assert item instanceof ServerStatus;
    ServerStatus api = (ServerStatus) item;
    ServerStatus existing = ServerStatus.get(time);
    evolveOrAdd(time, existing, api);
  }

  @Override
  protected ESIRefServerResult<GetStatusOk> getServerData(ESIRefClientProvider cp) throws ApiException, IOException {
    StatusApi apiInstance = cp.getStatusApi();
    ESIRefThrottle.throttle(endpoint().name());
    ApiResponse<GetStatusOk> result = apiInstance.getStatusWithHttpInfo(null, null);
    checkCommonProblems(result);
    return new ESIRefServerResult<>(extractExpiry(result, OrbitalProperties.getCurrentTime() + maxDelay()), result.getData());
  }

  @SuppressWarnings("RedundantThrows")
  @Override
  protected void processServerData(long time, ESIRefServerResult<GetStatusOk> data,
                                   List<RefCachedData> updates) throws IOException {
    GetStatusOk serverData = data.getData();
    // VIP is optional and may be null
    boolean vip = serverData.getVip() == null ? false : serverData.getVip();
    updates.add(new ServerStatus(serverData.getPlayers(), serverData.getStartTime()
                                                                    .getMillis(), serverData.getServerVersion(), vip));
  }

}
