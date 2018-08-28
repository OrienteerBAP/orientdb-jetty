package org.orienteer.jetty;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.function.Consumer;

public abstract class AbstractOrientDbJettyTest {


    @BeforeClass
    public static void initDatabase() {
        String url = "memory:testdb";
        String user = "admin";
        String password = "admin";

        System.setProperty("url", url);
        System.setProperty("admin.username", user);
        System.setProperty("admin.password", password);

        ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);

        if (!db.exists()) {
            db.create();
        }
        if (db.isClosed()) {
            db.open(user, password);
        }
        OrientDbJettyModule.initSchema(db);
        db.close();
    }

    @AfterClass
    public static void removeDatabase() {
        ODatabaseDocumentTx db = new ODatabaseDocumentTx(OrientDbJettyModule.getDatabaseUrl());
        db.open(OrientDbJettyModule.getUser(), OrientDbJettyModule.getPassword());

        if (db.exists()) {
            db.drop();
        }
    }


    protected void sudoExecute(Consumer<ODatabaseDocument> consumer) {
        String url = OrientDbJettyModule.getDatabaseUrl();
        String user = OrientDbJettyModule.getUser();
        String password = OrientDbJettyModule.getPassword();

        ODatabaseDocument db = new ODatabaseDocumentTx(url).open(user, password);
        try {
            consumer.accept(db);
        } finally {
            db.commit();
            db.close();
        }
    }
}
