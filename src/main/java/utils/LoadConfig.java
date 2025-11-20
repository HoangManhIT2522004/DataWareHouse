package utils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class LoadConfig {

    private final Document xmlDoc;

    public LoadConfig(String resourcePath) throws Exception {
        // Lấy file từ resources trong JAR
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new FileNotFoundException("Resource not found: " + resourcePath);
            }
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            xmlDoc = dBuilder.parse(is);
            xmlDoc.getDocumentElement().normalize();
        }
    }

    /** Lấy Element con đầu tiên từ Document root */
    public static Element getElement(Document doc, String tagName) {
        if (doc.getElementsByTagName(tagName).getLength() > 0) {
            return (Element) doc.getElementsByTagName(tagName).item(0);
        }
        return null;
    }

    /** Lấy Element con đầu tiên từ Element parent */
    public static Element getChildElement(Element parent, String tagName) {
        if (parent.getElementsByTagName(tagName).getLength() > 0) {
            return (Element) parent.getElementsByTagName(tagName).item(0);
        }
        return null;
    }

    /** Lấy giá trị text của Element con */
    public static String getValue(Element parent, String tagName) {
        if (parent == null) return "";
        Element child = getChildElement(parent, tagName);
        return child != null ? child.getTextContent().trim() : "";
    }

    public Document getXmlDoc() {
        return xmlDoc;
    }
}

