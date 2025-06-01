package ru.sergey310872.config;

import java.io.InputStream;
import java.util.Properties;

public class PropertiesFile {
    public static final Properties PROP;

    static {
        PROP = new Properties();
        try (InputStream input = PropertiesFile.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            PROP.load(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    private Properties prop;
//
//    public PropertiesFile() {
//        this.prop = new Properties();
//        try (InputStream input = PropertiesFile.class.getClassLoader()
//                .getResourceAsStream("application.properties")) {
//            this.prop.load(input);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    public Properties getProp() {
//        return prop;
//    }
}
