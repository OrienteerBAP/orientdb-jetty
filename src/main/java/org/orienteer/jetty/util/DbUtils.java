package org.orienteer.jetty.util;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.eclipse.jetty.server.session.SessionData;
import org.eclipse.jetty.util.ClassLoadingObjectInputStream;
import org.orienteer.jetty.OrientDbJettyModule;
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
import java.util.stream.Collectors;

import static java.util.Optional.empty;

/**
 * Utility class for work with database
 */
public final class DbUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DbUtils.class);

    private DbUtils() {}

    /**
     * Convert data to OrientDB document and save it in database
     * @param db {@link ODatabaseDocument} database
     * @param id {@link String} session id
     * @param data {@link SessionData} session data which need save
     */
    public static void storeSession(ODatabaseDocument db, String id, SessionData data) {
        Optional<ODocument> docOpt = getSessionDocumentById(db, id);
        ODocument doc = docOpt.orElse(new ODocument(OrientDbJettyModule.SESSION_DATA_CLASS_NAME));
        if (doc.getIdentity().isNew()) {
            doc.field(OrientDbJettyModule.PROP_ID, id);
        }
        doc.field(OrientDbJettyModule.PROP_DATA, toBytes(data));
        doc.field(OrientDbJettyModule.PROP_EXPIRY_TIME, data.getExpiry());
        doc.save();
    }

    /**
     * Search session data by id.
     * Invokes {@link DbUtils#getSessionDocumentById(ODatabaseDocument, String)} and
     * then convert field {@link OrientDbJettyModule#PROP_DATA} to {@link SessionData}
     * @param db {@link ODatabaseDocument} database
     * @param id {@link String} session id
     * @return {@link Optional<SessionData>} or {@link Optional#empty()} if can't load session data by given id
     */
    public static Optional<SessionData> getSessionById(ODatabaseDocument db, String id) {
        return getSessionDocumentById(db, id)
                .flatMap(d -> fromBytes(d.field(OrientDbJettyModule.PROP_DATA)));
    }

    /**
     * Search session data document by given id
     * @param db {@link ODatabaseDocument} database which uses for query
     * @param id {@link String} session data id
     * @return {@link Optional<ODocument>} which contains document of session data
     * or {@link Optional#empty()} if no session data with given id
     */
    public static Optional<ODocument> getSessionDocumentById(ODatabaseDocument db, String id) {
        String sql = String.format("select from %s where %s = ?", OrientDbJettyModule.SESSION_DATA_CLASS_NAME, OrientDbJettyModule.PROP_ID);
        List<OIdentifiable> identifiables = db.query(new OSQLSynchQuery<>(sql, 1), id);

        return Optional.ofNullable(identifiables)
                .filter(list -> !list.isEmpty())
                .map(list -> list.get(0).getRecord());
    }

    /**
     * Search expired sessions id in provided candidates
     * @param db {@link ODatabaseDocument} database
     * @param candidates {@link Set<String>} id of sessions which can be expired
     * @return {@link Set<String>} set of id expired sessions
     */
    public static Set<String> getExpiredSessions(ODatabaseDocument db, Set<String> candidates) {
        String sql = String.format("select %s from %s where %s in ? and %s <= ?",
                OrientDbJettyModule.PROP_ID, OrientDbJettyModule.SESSION_DATA_CLASS_NAME, OrientDbJettyModule.PROP_ID,
                OrientDbJettyModule.PROP_EXPIRY_TIME);
        long now = System.currentTimeMillis();
        List<OIdentifiable> identifiables = db.query(new OSQLSynchQuery<>(sql), candidates, now);
        return identifiables == null || identifiables.isEmpty() ? Collections.<String>emptySet() : toIdSet(identifiables);
    }

    /**
     * Delete session data by given id
     * @param db {@link ODatabaseDocument} database
     * @param id {@link String} session id
     */
    public static void deleteSessionById(ODatabaseDocument db, String id) {
        String sql = String.format("delete from %s where %s = ?", OrientDbJettyModule.SESSION_DATA_CLASS_NAME,
                OrientDbJettyModule.PROP_ID);
        db.command(new OCommandSQL(sql)).execute(id);
    }

    /**
     * Check if session with given id exists in database
     * @param db {@link ODatabaseDocument} database
     * @param id {@link String} session id
     * @return tru if session exists in database
     */
    public static boolean isSessionExistsById(ODatabaseDocument db, String id) {
        String sql = String.format("select %s from %s where %s = ?", OrientDbJettyModule.PROP_ID,
                OrientDbJettyModule.SESSION_DATA_CLASS_NAME, OrientDbJettyModule.PROP_ID);
        List<OIdentifiable> identifiables = db.query(new OSQLSynchQuery<>(sql, 1), id);
        return identifiables != null && !identifiables.isEmpty();
    }

    /**
     * Serialize data to bytes.
     * @param data {@link SessionData} for serialize it
     * @return array of bytes or throw exception if can't convert data to bytes
     * @throws IllegalStateException if can't serialize data
     */
    private static byte[] toBytes(SessionData data) throws IllegalStateException {
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
    private static Set<String> toIdSet(List<OIdentifiable> identifiables) {
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
    private static Optional<SessionData> fromBytes(byte [] bytes) {
        try (ObjectInputStream in = new ClassLoadingObjectInputStream(new ByteArrayInputStream(bytes))) {
            return Optional.of((SessionData) in.readObject());
        } catch (Exception e) {
            LOG.error("Can't read {} from byte array!", SessionData.class.getName(), e);
        }
        return empty();
    }
}
