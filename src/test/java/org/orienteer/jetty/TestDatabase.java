package org.orienteer.jetty;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDatabase extends AbstractOrientDbJettyTest {

    @Test
    public void testInitSchema() {
        sudoExecute(db -> {
            OSchema schema = db.getMetadata().getSchema();

            assertTrue(schema.existsClass(OrientDbJettyModule.SESSION_DATA_CLASS_NAME));

            OClass oClass = schema.getClass(OrientDbJettyModule.SESSION_DATA_CLASS_NAME);

            assertExistsProperty(oClass, OrientDbJettyModule.PROP_ID);
            assertExistsProperty(oClass, OrientDbJettyModule.PROP_DATA);
            assertExistsProperty(oClass, OrientDbJettyModule.PROP_EXPIRY_TIME);

            assertPropertyType(oClass, OrientDbJettyModule.PROP_ID, OType.STRING);
            assertPropertyType(oClass, OrientDbJettyModule.PROP_DATA, OType.BINARY);
            assertPropertyType(oClass, OrientDbJettyModule.PROP_EXPIRY_TIME, OType.LONG);

        });
    }

    private void assertExistsProperty(OClass oClass, String name) {
        String err = String.format("Property `%s.%s` doesn't exists in database!", oClass.getName(), name);
        assertTrue(err, oClass.existsProperty(name));
    }

    private void assertPropertyType(OClass oClass, String name, OType type) {
        String err = String.format("Property `%s.%s` doesn't have a type `%s`!", oClass.getName(), name, type.name());
        assertEquals(err, type, oClass.getProperty(name).getType());
    }
}
