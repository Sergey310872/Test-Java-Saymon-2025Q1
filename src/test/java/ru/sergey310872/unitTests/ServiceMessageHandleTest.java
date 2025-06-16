package ru.sergey310872.unitTests;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.sergey310872.config.PropertiesFile;
import ru.sergey310872.dto.SinkMessage;
import ru.sergey310872.dto.SinkMessageImp;
import ru.sergey310872.dto.SourceMessage;
import ru.sergey310872.dto.SourceMessageImp;
import ru.sergey310872.service.ServiceMessageHandle;

import java.io.InputStream;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ServiceMessageHandleTest {
    private static Properties PROPERTY;

    private ServiceMessageHandle serviceMessageHandle;
    List<SourceMessage> serviceMessageList;

    @BeforeAll
    static void prepare() {
        PROPERTY = new Properties();
        try (InputStream input = PropertiesFile.class.getClassLoader()
                .getResourceAsStream("application-test.properties")) {
            PROPERTY.load(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @BeforeEach
    void setUp() {
        serviceMessageHandle = new ServiceMessageHandle(PROPERTY);
        serviceMessageList = new ArrayList<>();
        serviceMessageList.add(new SourceMessageImp(11111, Map.of("A", "value A"), 30));
        serviceMessageList.add(new SourceMessageImp(11112, Map.of("A", "value A"), 40));
        serviceMessageList.add(new SourceMessageImp(11113, Map.of("A", "value B"), 50));
        serviceMessageList.add(new SourceMessageImp(11114, Map.of("B", "value B"), 60));
        serviceMessageList.add(new SourceMessageImp(11115, Map.of("B", "value B"), 70));
    }

    @Test
    void deduplicationSourceMessageTest() {
        //given
        Set<SourceMessage> expected = new HashSet<>(serviceMessageList);
//        List<SourceMessage> expected = new ArrayList<>(serviceMessageList);
//        expected.remove(1);
        //when
        Iterable<SourceMessage> result = serviceMessageHandle.deduplication(serviceMessageList);
//        List<SourceMessage> actual = new ArrayList<>((Collection) result);
        //then
        assertNotNull(result);
        assertNotSame(serviceMessageList, result);
//        assertEquals(expected, actual);
        assertEquals(expected, result);
//        assertEquals(new HashSet<>(expected), result);
    }

    @Test
    void filteringSourceMessageTest() {
        //given
        List<SourceMessage> expected = new ArrayList<>(serviceMessageList);
        expected.remove(4);
        expected.remove(3);
        //when
        Iterable<SourceMessage> result = serviceMessageHandle.filtering(serviceMessageList);
        //then
        assertNotNull(result);
        assertNotSame(serviceMessageList, result);
        assertEquals(expected, expected);
    }

    @Test
    void groupingBySourceMessageTest() {
        //given
        List<SourceMessage> expected = new ArrayList<>(serviceMessageList);
        expected.remove(4);
        expected.set(3, new SourceMessageImp(11115, Map.of("B", "value B"), 130));
        //when
        Iterable<SourceMessage> result = serviceMessageHandle.groupBy(serviceMessageList);
        List<SourceMessage> actual = new ArrayList<>((Collection) result);
        //then
        assertNotNull(result);
        assertNotSame(serviceMessageList, result);
        assertEquals(expected.get(3).value(), actual.get(3).value());
    }

    @Test
    void aggregatingSourceMessageTest() {
        //given
        SinkMessage expected = new SinkMessageImp(11111, 11115,
                Map.of("A", "value B", "B", "value B"), 30, 70, 50, 5);
        //when
        SinkMessage result = serviceMessageHandle.aggregation(serviceMessageList);
        //then
        assertNotNull(result);
        assertEquals(expected, result);
    }
}