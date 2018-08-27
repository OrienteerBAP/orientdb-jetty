package org.orienteer.jetty;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.eclipse.jetty.server.session.AbstractSessionDataStore;
import org.eclipse.jetty.server.session.SessionData;
import org.eclipse.jetty.util.ClassLoadingObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

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
        if (pool != null) {
            synchronized (this) {
                pool.close();
            }
        }
    }

    @Override
    public void doStore(String id, SessionData data, long lastSaveTime) throws Exception {
        sudoExecute(db -> {
            Optional<ODocument> docOpt = loadById(db, id);
            ODocument doc = docOpt.orElse(new ODocument(OrientDbJettyModule.SESSION_DATA_CLASS_NAME));
            if (doc.getIdentity().isNew()) {
                doc.field(OrientDbJettyModule.PROP_ID, id);
            }
            doc.field(OrientDbJettyModule.PROP_DATA, toBytes(data));
            doc.field(OrientDbJettyModule.PROP_EXPIRY_TIME, data.getExpiry());
            doc.save();
        });
    }

    @Override
    public Set<String> doGetExpired(Set<String> candidates) {
        return sudoGet(db -> {
            String sql = String.format("select %s from %s where %s in ? and %s <= ?",
                    OrientDbJettyModule.PROP_ID, OrientDbJettyModule.SESSION_DATA_CLASS_NAME, OrientDbJettyModule.PROP_ID,
                    OrientDbJettyModule.PROP_EXPIRY_TIME);
            long now = System.currentTimeMillis();
            List<OIdentifiable> identifiables = db.query(new OSQLSynchQuery<>(sql), candidates, now);
            return identifiables == null || identifiables.isEmpty() ? Collections.<String>emptySet() : toIdSet(identifiables);
        }).orElse(Collections.emptySet());
    }

    @Override
    public boolean isPassivating() {
        return true;
    }

    @Override
    public boolean exists(String id) throws Exception {
        return sudoGet(db -> {
            String sql = String.format("select %s from %s where %s = ?", OrientDbJettyModule.PROP_ID,
                OrientDbJettyModule.SESSION_DATA_CLASS_NAME, OrientDbJettyModule.PROP_ID);
            List<OIdentifiable> identifiables = db.query(new OSQLSynchQuery<>(sql, 1), id);
            return identifiables != null && !identifiables.isEmpty();
        }).orElse(false);
    }

    @Override
    public SessionData load(String id) throws Exception {
        return sudoGet(db -> loadById(db, id)
                .flatMap(d -> fromBytes(d.field(OrientDbJettyModule.PROP_DATA)))
                .orElse(null)
        ).orElse(null);
    }

    @Override
    public boolean delete(String id) throws Exception {
        sudoExecute(db -> {
            String sql = String.format("delete from %s where %s = ?", OrientDbJettyModule.SESSION_DATA_CLASS_NAME,
                    OrientDbJettyModule.PROP_ID);
            db.command(new OCommandSQL(sql)).execute(id);
        });
        return true;
    }

    /**
     * Serialize data to bytes.
     * @param data {@link SessionData} for serialize it
     * @return array of bytes or throw exception if can't convert data to bytes
     * @throws IllegalStateException if can't serialize data
     */
    private byte[] toBytes(SessionData data) throws IllegalStateException {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(out);
            oos.writeObject(data);
            oos.close();
            return out.toByteArray();
        } catch (Exception e) {
            throw new IllegalStateException("Can't serialize " + data, e);
        }
    }

    /**
     * Convert list of indentifiables to set of ids.
     * Uses {@link OrientDbJettyModule#PROP_ID} for get value which stores in field id.
     * @param identifiables {@link List<OIdentifiable>}
     * @return {@link Set<String>} set of id
     */
    private Set<String> toIdSet(List<OIdentifiable> identifiables) {
        return identifiables.stream()
                .map(i -> (ODocument) i.getRecord())
                .map(d -> (String) d.field(OrientDbJettyModule.PROP_ID))
                .collect(Collectors.toSet());
    }

    /**
     * Deserialize bytes to {@link SessionData}.
     * @param bytes byte array for deserialize
     * @return {@link Optional<SessionData>} which contains {@link SessionData} or {@link Optional#empty()} if can't deserialize data
     */
    private Optional<SessionData> fromBytes(byte [] bytes) {
        try (ObjectInputStream in = new ClassLoadingObjectInputStream(new ByteArrayInputStream(bytes))) {
            return Optional.of((SessionData) in.readObject());
        } catch (Exception e) {
            LOG.error("Can't read {} from byte array!", SessionData.class.getName(), e);
        }
        return empty();
    }

    /**
     * Search session data document by given id
     * @param db {@link ODatabaseDocument} database which uses for query
     * @param id {@link String} session data id
     * @return {@link Optional<ODocument>} which contains document of session data
     * or {@link Optional#empty()} if no session data with given id
     */
    private Optional<ODocument> loadById(ODatabaseDocument db, String id) {
        String sql = String.format("select from %s where %s = ?", OrientDbJettyModule.SESSION_DATA_CLASS_NAME, OrientDbJettyModule.PROP_ID);
        List<OIdentifiable> identifiables = db.query(new OSQLSynchQuery<>(sql, 1), id);

        return Optional.ofNullable(identifiables)
                .filter(list -> !list.isEmpty())
                .map(list -> list.get(0).getRecord());
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
        if (pool == null) {
            synchronized (this) {
                if (pool == null) {
                    String username = System.getProperty("admin.username");
                    String password = System.getProperty("admin.password");
                    String url = System.getProperty("remote.url");
                    if (username != null && password != null && url != null) {
                        pool = new OPartitionedDatabasePool(url, username, password);
                    }
                }
            }
        }

        try {
            return pool != null ? ofNullable(pool.acquire()) : empty();
        } catch (Exception ex) {
            LOG.error("Can't acquire database from pool!", ex);
        }
        return empty();
    }
}
