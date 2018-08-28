package org.orienteer.jetty;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.eclipse.jetty.server.session.AbstractSessionDataStore;
import org.eclipse.jetty.server.session.SessionData;
import org.orienteer.jetty.util.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;

/**
 * Implementation of {@link AbstractSessionDataStore} for store session data in OrientDB.
 * Uses database pool for access to database.
 */
public class OrientDbSessionDataStore extends AbstractSessionDataStore {

    private static final Logger LOG = LoggerFactory.getLogger(OrientDbSessionDataStore.class);

    /**
     * Database pool
     */
    private OPartitionedDatabasePool pool;

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        closePool();
    }

    @Override
    public void doStore(String id, SessionData data, long lastSaveTime) throws Exception {
        sudoExecute(db -> DbUtils.storeSession(db, id, data));
    }

    @Override
    public Set<String> doGetExpired(Set<String> candidates) {
        return sudoGet(db -> DbUtils.getExpiredSessions(db, candidates))
                .orElse(Collections.emptySet());
    }

    @Override
    public boolean isPassivating() {
        return true;
    }

    @Override
    public boolean exists(String id) throws Exception {
        return sudoGet(db -> DbUtils.isSessionExistsById(db, id))
                .orElse(false);
    }

    @Override
    public SessionData load(String id) throws Exception {
        return sudoGet(db ->
                DbUtils.getSessionById(db, id).orElse(null)
        ).orElse(null);
    }

    @Override
    public boolean delete(String id) throws Exception {
        sudoExecute(db -> DbUtils.deleteSessionById(db, id));
        return true;
    }


    /**
     * Execute get function and close database.
     * @param func function for execute
     * @param <T> type of return value
     * @return value from function
     */
    private <T> Optional<T> sudoGet(Function<ODatabaseDocument, T> func) {
        return getDatabase()
                .map(db -> {
                    try {
                        return func.apply(db);
                    } finally {
                        db.commit();
                        db.close();
                    }
                });
    }

    /**
     * Execute some action and close database
     * @param func function for execute
     */
    private void sudoExecute(Consumer<ODatabaseDocument> func) {
        getDatabase()
                .ifPresent(db -> {
                    try {
                        func.accept(db);
                    } finally {
                        db.commit();
                        db.close();
                    }
                });
    }

    /**
     * Get database.
     * Creates {@link OrientDbSessionDataStore#pool} if it is null.
     * For create database pool requires System properties:
     * 1. admin.username - username of admin user
     * 2. admin.password - password of admin user
     * 3. remote.url - remote url for connect to database
     * @return {@link Optional<ODatabaseDocument>} database or {@link Optional#empty()} if can't acquire database;
     */
    private Optional<ODatabaseDocument> getDatabase() {
        try {
            return acquirePool().map(OPartitionedDatabasePool::acquire);
        } catch (Exception ex) {
            // Exception throws when connection to database was lost
            // Close pool and try open it again
            closePool();
            try {
                return acquirePool().map(OPartitionedDatabasePool::acquire);
            } catch (Exception e) {
                LOG.error("Can't acquire database from pool!", ex);
                // Close pool if it is not null
                closePool();
            }
        }
        return empty();
    }

    private Optional<OPartitionedDatabasePool> acquirePool() {
        if (pool == null) {
            synchronized (this) {
                if (pool == null) {
                    String username = OrientDbJettyModule.getUser();
                    String password = OrientDbJettyModule.getPassword();
                    String url = OrientDbJettyModule.getDatabaseUrl();
                    if (username != null && password != null && url != null) {
                        pool = new OPartitionedDatabasePool(url, username, password);
                    }
                }
            }
        }
        return ofNullable(pool);
    }

    private void closePool() {
        if (pool != null) {
            synchronized (this) {
                if (pool != null) {
                    pool.close();
                    pool = null;
                }
            }
        }
    }
}
