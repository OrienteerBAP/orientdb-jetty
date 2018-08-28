package org.orienteer.jetty;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.session.DefaultSessionCache;
import org.eclipse.jetty.server.session.Session;
import org.eclipse.jetty.server.session.SessionData;
import org.eclipse.jetty.server.session.SessionHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.orienteer.jetty.util.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.junit.Assert.*;

public class TestStoreSession extends AbstractOrientDbJettyTest {

    private static final Logger LOG = LoggerFactory.getLogger(TestStoreSession.class);

    private Server server;
    private SessionHandler handler;

    @Before
    public void before() throws Exception {
        server = new Server();
        handler = new SessionHandler();

        server.setHandler(handler);
        server.addBean(new JettyOrientDbSessionDataStoreFactory());

        DefaultSessionCache cache = new DefaultSessionCache(handler);
        cache.setSessionDataStore(new OrientDbSessionDataStore());

        handler.setMaxInactiveInterval(1);

        server.start();
    }

    @After
    public void after() throws Exception {
        server.stop();
    }

    @Test
    public void testOrientDbSessionStore() throws Exception {
        handler.start();

        Session session = (Session) handler.newHttpSession(new Request(null, null));
        session.setAttribute("one", 1);
        session.setAttribute("two", 2);

        handler.stop();

        sudoExecute(db -> assertSavedSession(db, session));

        handler.start();
        Session restoredSession = handler.getSession(session.getId());
        assertEquals(session.getId(), restoredSession.getId());
        assertEquals(session.getAttribute("one"), restoredSession.getAttribute("one"));
        assertEquals(session.getAttribute("two"), restoredSession.getAttribute("two"));

        handler.invalidate(session.getId());

        sudoExecute(db -> assertDeletedSession(db, session));
    }

    private void assertSavedSession(ODatabaseDocument db, Session session) {
        Optional<SessionData> sessionByIdOpt = DbUtils.getSessionById(db, session.getId());

        assertTrue(String.format("Session with id '%s' not present", session.getId()), sessionByIdOpt.isPresent());

        SessionData sessionData = sessionByIdOpt.get();
        assertEquals("Session id not equals!", session.getId(), sessionData.getId());
        assertEquals("Session attribute 'one' not equals", session.getAttribute("one"), sessionData.getAttribute("one"));
        assertEquals("Session attribute 'two' not equals", session.getAttribute("two"), sessionData.getAttribute("two"));
    }


    private void assertDeletedSession(ODatabaseDocument db, Session session) {
        Optional<SessionData> empty = DbUtils.getSessionById(db, session.getId());
        assertFalse("Expired session not deleted", empty.isPresent());
    }

}
