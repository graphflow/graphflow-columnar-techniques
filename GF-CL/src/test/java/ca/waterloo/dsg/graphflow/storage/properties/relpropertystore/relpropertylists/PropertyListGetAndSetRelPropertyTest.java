package ca.waterloo.dsg.graphflow.storage.properties.relpropertystore.relpropertylists;

import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PropertyListGetAndSetRelPropertyTest {

    @Test
    public void PropertyListIntegerGetAndSetPropertyTest() {
        var propertyList = new RelPropertyListInteger(
            new int[0][0] /*num elements in each bucket*/, 0 /*label*/, 0 /*srcVertexType*/);

        /*common scenario test, where the bucket slot is already allocated*/
        propertyList.setProperty(0, 0, 314);
        Assertions.assertEquals(314, propertyList.getProperty(0, 0));

        /*tests for increasing the size of a particular bucket.*/
        propertyList.setProperty(0, 1024, 415);
        Assertions.assertEquals(415, propertyList.getProperty(0, 1024));

        /*tests for allocating a bucket slot that has not been already allocated.*/
        propertyList.setProperty(5, 0, 835);
        Assertions.assertEquals(835, propertyList.getProperty(5, 0));

        /*tests that a particular property location that has not been set return null equivalent
        of each corresponding dataType.*/
        Assertions.assertEquals(DataType.NULL_INTEGER, propertyList.getProperty(0, 2));

        /*tests for the non-existence of the referenced buckets in the existing bucket slot.
        Accessing this location should return the null equivalent of the respective dataType.*/
        Assertions.assertEquals(DataType.NULL_INTEGER, propertyList.getProperty(10, 14));

        /*tests for the non-existence of the bucket slot. Accessing this location should
        return the null equivalent of the respective dataType.*/
        Assertions.assertEquals(DataType.NULL_INTEGER, propertyList.getProperty(2532524652L, 234));
    }

    @Test
    public void PropertyListDoubleGetAndSetPropertyTest() {
        var propertyList = new RelPropertyListDouble(
            new int[0][0] /*num elements in each bucket*/, 0 /*label*/, 0 /*srcVertexType*/);

        propertyList.setProperty(0, 0, 31.4);
        Assertions.assertEquals(31.4, propertyList.getProperty(0, 0), DataType.DELTA);

        propertyList.setProperty(0, 1024, 4.15);
        Assertions.assertEquals(4.15, propertyList.getProperty(0, 1024), DataType.DELTA);

        propertyList.setProperty(5, 0, 0.835);
        Assertions.assertEquals(0.835, propertyList.getProperty(5, 0), DataType.DELTA);

        Assertions.assertEquals(DataType.NULL_DOUBLE, propertyList.getProperty(0, 2), DataType.
            DELTA);
        Assertions.assertEquals(DataType.NULL_DOUBLE, propertyList.getProperty(10, 14), DataType.
            DELTA);
        Assertions.assertEquals(DataType.NULL_DOUBLE, propertyList.getProperty(2532524652L, 234),
            DataType.DELTA);
    }

    @Test
    public void PropertyListStringGetAndSetPropertyTest() {
        var propertyList = new RelPropertyListString(
            new int[0][0] /*num elements in each bucket*/, 0 /*label*/, 0 /*srcVertexType*/);

        propertyList.setProperty(0, 0, "hello".getBytes());
        Assertions.assertEquals("hello", propertyList.getProperty(0, 0));

        propertyList.setProperty(0, 1024, "whatsup".getBytes());
        Assertions.assertEquals("whatsup", propertyList.getProperty(0, 1024));

        propertyList.setProperty(5, 0, "hey".getBytes());
        Assertions.assertEquals("hey", propertyList.getProperty(5, 0));

        Assertions.assertNull(propertyList.getProperty(0, 2));
        Assertions.assertNull(propertyList.getProperty(10, 14));
        Assertions.assertNull(propertyList.getProperty(2532524652L, 234));
    }

    @Test
    public void PropertyListBooleanColumnGetAndSetPropertyTest() {
        var propertyList = new RelPropertyListBoolean(
            new int[0][0] /*num elements in each bucket*/, 0 /*label*/, 0 /*srcVertexType*/);

        propertyList.setProperty(0, 0, true);
        Assertions.assertTrue(propertyList.getProperty(0, 0));

        propertyList.setProperty(0, 1024, false);
        Assertions.assertFalse(propertyList.getProperty(0, 1024));

        propertyList.setProperty(5, 0, true);
        Assertions.assertTrue(propertyList.getProperty(5, 0));

        Assertions.assertFalse(propertyList.getProperty(0, 2));
        Assertions.assertFalse(propertyList.getProperty(10, 14));
        Assertions.assertFalse(propertyList.getProperty(2532524652L, 234));
    }
}
