package utils;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class LoadConfig {

    private final Document xmlDoc;

    public LoadConfig(String configPath) throws Exception {
        InputStream is = null;

        try {
            // Thử đọc từ filesystem trước (ưu tiên)
            File file = new File(configPath);
            if (file.exists()) {
                is = new FileInputStream(file);
                System.out.println("[LoadConfig] Loading from filesystem: " + file.getAbsolutePath());
            } else {
                // Nếu không tìm thấy trong filesystem, thử đọc từ classpath/resources
                is = getClass().getClassLoader().getResourceAsStream(configPath);
                if (is == null) {
                    throw new FileNotFoundException("Config not found in filesystem or resources: " + configPath);
                }
                System.out.println("[LoadConfig] Loading from classpath: " + configPath);
            }

            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            xmlDoc = dBuilder.parse(is);
            xmlDoc.getDocumentElement().normalize();

        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (Exception e) {
                    // Ignore
                }
            }
        }
    }

    /** Lấy Element con đầu tiên từ Document root */
    public static Element getElement(Document doc, String tagName) {
        NodeList nodeList = doc.getElementsByTagName(tagName);
        if (nodeList.getLength() > 0) {
            return (Element) nodeList.item(0);
        }
        return null;
    }

    /**
     * Lấy Element con đầu tiên từ Element parent
     */
    public static Element getChildElement(Element parent, String tagName) {
        if (parent == null) {
            return null;
        }

        // Chỉ lấy các element con trực tiếp, không tìm trong toàn bộ cây
        NodeList children = parent.getElementsByTagName(tagName);

        if (children.getLength() > 0) {
            return (Element) children.item(0);
        }

        return null;
    }

    /** Lấy giá trị text của Element con */
    public static String getValue(Element parent, String tagName) {
        if (parent == null) {
            return "";
        }
        Element child = getChildElement(parent, tagName);
        return child != null ? child.getTextContent().trim() : "";
    }

    public Document getXmlDoc() {
        return xmlDoc;
    }
}