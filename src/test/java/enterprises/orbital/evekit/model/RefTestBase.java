package enterprises.orbital.evekit.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.After;

import enterprises.orbital.db.ConnectionFactory.RunInVoidTransaction;
import enterprises.orbital.evekit.account.EveKitRefDataProvider;

public class RefTestBase extends SyncTestBase {

  protected static final AttributeSelector ANY_SELECTOR = new AttributeSelector("{ any: true }");

  protected static AttributeSelector makeAtSelector(
                                                    long time) {
    return new AttributeSelector("{values: [" + time + "]}");
  }

  @Override
  @After
  public void teardown() throws Exception {
    // Cleanup RefTracker, RefData and test specific tables after each test
    EveKitRefDataProvider.getFactory().runTransaction(new RunInVoidTransaction() {
      @Override
      public void run() throws Exception {
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM RefSyncTracker").executeUpdate();
        EveKitRefDataProvider.getFactory().getEntityManager().createQuery("DELETE FROM RefData").executeUpdate();
      }
    });

    super.teardown();
  }

  protected interface BatchRetriever<A extends RefCachedData> {
    public List<A> getNextBatch(
                                List<A> lastBatch);
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

}
