package org.orienteer.jetty;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.eclipse.jetty.server.session.AbstractSessionDataStore;
import org.eclipse.jetty.server.session.SessionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

import static java.util.Optional.of;

/**
 *
 */
public class OrientDbSessionDataStore extends AbstractSessionDataStore {

    private static final Logger LOG = LoggerFactory.getLogger(OrientDbSessionDataStore.class);

    private OPartitionedDatabasePool pool;

    @Override
    protected void doStart() throws Exception {
        super.doStart();
//        Class.forName("com.orientechnologies.orient.jdbc.OrientJdbcDriver");
    }

    @Override
    public void doStore(String id, SessionData data, long lastSaveTime) throws Exception {
        LOG.info("do store: {}", data);
//        ODatabaseDocumentTx db = new ODatabaseDocumentTx("remote:localhost/Orienteer");
//        db.open(System.getProperty("admin.username"), System.getProperty("admin.password"));

        getDatabase().ifPresent(db -> {
            LOG.info("is database open: {}", !db.isClosed());
            db.close();
        });


//        db.close();
//        Connection conn = (OrientJdbcConnection) DriverManager.getConnection("jdbc:orient:remote:localhost/Orienteer", info);
//        try {
//            PreparedStatement ps = conn.prepareStatement("select from OUser");
//            ResultSet resultSet = ps.executeQuery();
//            LOG.info("size: {}", resultSet.getFetchSize());
//        } finally {
//            conn.close();
//        }
    }

    @Override
    public Set<String> doGetExpired(Set<String> candidates) {
        return candidates;
    }

    @Override
    public boolean isPassivating() {
        return true;
    }

    @Override
    public boolean exists(String id) throws Exception {
        return false;
    }

    @Override
    public SessionData load(String id) throws Exception {
        return null;
    }

    @Override
    public boolean delete(String id) throws Exception {
        return true;
    }



    private Optional<ODatabaseDocument> getDatabase() {
        if (pool == null) {
            synchronized (this) {
                if (pool == null) {
                    String username = System.getProperty("admin.username");
                    String password = System.getProperty("admin.password");
                    pool = new OPartitionedDatabasePool("remote:localhost/Orienteer", username, password);
                }
            }
        }

        return of(pool.acquire());
    }
}
