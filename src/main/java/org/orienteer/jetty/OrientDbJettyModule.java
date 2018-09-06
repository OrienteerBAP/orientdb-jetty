package org.orienteer.jetty;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Module which need for initialize schema
 */
public final class OrientDbJettyModule {

    /**
     * OrientDB class name of session data
     */
    public static final String SESSION_DATA_CLASS_NAME = "OSessionData";

    /**
     * {@link OType#STRING}
     * Session data id
     */
    public static final String PROP_ID = "id";

    /**
     * {@link OType#BINARY}
     * Serialized session data. Stores as byte array
     */
    public static final String PROP_DATA = "data";

    /**
     * {@link OType#LONG}
     * Time in milliseconds when session data will be expired
     */
    public static final String PROP_EXPIRY_TIME = "expiryTime";

    /**
     * Init schema. Need call after application was started and database was acquired.
     * @param db {@link ODatabaseDocument} database
     * @return {@link OClass} of session data
     */
    public static OClass initSchema(ODatabaseDocument db) {
        OSchema schema = db.getMetadata().getSchema();
        OClass oClass = schema.getClass(SESSION_DATA_CLASS_NAME);
        if (oClass == null) {
            oClass = schema.createClass(SESSION_DATA_CLASS_NAME);
        }

        OProperty id = createPropertyIfNotExists(oClass, PROP_ID, OType.STRING);
        id.setNotNull(true);

        OProperty data = createPropertyIfNotExists(oClass, PROP_DATA, OType.BINARY);
        data.setNotNull(true);

        OProperty expiry = createPropertyIfNotExists(oClass, PROP_EXPIRY_TIME, OType.LONG);
        expiry.setNotNull(true);

        return oClass;
    }

    public static String getDatabaseUrl() {
        return System.getProperty("remote.url");
    }

    public static String getUser() {
        return System.getProperty("admin.username");
    }

    public static String getPassword() {
        return System.getProperty("admin.password");
    }

    /**
     * Create property if it doesn't exists or update exists property
     * @param oClass {@link OClass} session data class
     * @param name {@link String} property name
     * @param type {@link OType} property type
     * @return {@link OProperty}
     */
    private static OProperty createPropertyIfNotExists(OClass oClass, String name, OType type) {
        OProperty property = oClass.getProperty(name);
        if (property == null) {
            property = oClass.createProperty(name, type);
        } else {
            property.setType(type);
        }
        return property;
    }


    private OrientDbJettyModule() {}
}
