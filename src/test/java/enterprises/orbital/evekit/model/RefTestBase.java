package enterprises.orbital.evekit.model;

import enterprises.orbital.evekit.account.EveKitRefDataProvider;
import org.junit.After;
import org.junit.Assert;

import java.util.*;

public class RefTestBase extends SyncTestBase {

  @Override
  @After
  public void teardown() throws Exception {
    // Cleanup RefTracker, RefData and test specific tables after each test
    EveKitRefDataProvider.getFactory()
                         .runTransaction(() -> EveKitRefDataProvider.getFactory()
                                                                    .getEntityManager()
                                                                    .createQuery("DELETE FROM ESIRefEndpointSyncTracker")
                                                                    .executeUpdate());
    super.teardown();
  }

  protected interface BatchRetriever<A extends RefCachedData> {
    List<A> getNextBatch(List<A> lastBatch);
  }

  protected <A extends RefCachedData> List<A> retrieveAll(
      BatchRetriever<A> retriever) {
    List<A> results = new ArrayList<>();
    List<A> nextBatch = retriever.getNextBatch(Collections.emptyList());
    while (!nextBatch.isEmpty()) {
      results.addAll(nextBatch);
      nextBatch = retriever.getNextBatch(nextBatch);
    }
    return results;
  }

  protected static Map<String, List<String>> createHeaders(String... pairs) {
    Assert.assertTrue(pairs.length % 2 == 0);
    Map<String, List<String>> mm = new HashMap<>();
    for (int i = 0; i < pairs.length; i += 2) {
      mm.put(pairs[i], Collections.singletonList(pairs[i + 1]));
    }
    return mm;
  }

}
