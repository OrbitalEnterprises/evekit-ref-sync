package enterprises.orbital.evekit.model;

import enterprises.orbital.evekit.account.EveKitRefDataProvider;
import org.junit.After;
import org.junit.Before;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.base.PersistentProperty;
import enterprises.orbital.db.DBPropertyProvider;
import enterprises.orbital.evekit.account.EveKitUserAccountProvider;

public class SyncTestBase {

  @Before
  public void setup() throws Exception {
    OrbitalProperties.addPropertyFile("SyncTest.properties");
    PersistentProperty.setProvider(new DBPropertyProvider(OrbitalProperties.getGlobalProperty(EveKitRefDataProvider.REF_DATA_PU_PROP)));
  }

  @After
  public void teardown() throws Exception {}

}
