package org.orienteer.jetty;

import org.eclipse.jetty.server.session.AbstractSessionDataStoreFactory;
import org.eclipse.jetty.server.session.SessionDataStore;
import org.eclipse.jetty.server.session.SessionHandler;

/**
 *
 */
public class JettyOrientDbSessionDataStoreFactory extends AbstractSessionDataStoreFactory {
    @Override
    public SessionDataStore getSessionDataStore(SessionHandler handler) throws Exception {
        return new OrientDbSessionDataStore();
    }
}
