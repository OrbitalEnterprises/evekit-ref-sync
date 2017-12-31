package enterprises.orbital.evekit.model;

import enterprises.orbital.eve.esi.client.api.AllianceApi;
import enterprises.orbital.eve.esi.client.api.FactionWarfareApi;
import enterprises.orbital.eve.esi.client.api.SovereigntyApi;
import enterprises.orbital.eve.esi.client.api.StatusApi;

import java.util.concurrent.ExecutorService;

/**
 * Implementations of this class are used to provide properly configured ESI clients
 * to synchronization code.  This allows for the abstraction of initialization code, and
 * also makes it easier to inject test code.
 */
public interface ESIClientProvider {
  ExecutorService getScheduler();
  StatusApi getStatusApi();
  AllianceApi getAllianceApi();
  SovereigntyApi getSovereigntyApi();
  FactionWarfareApi getFactionWarfareApi();
}
