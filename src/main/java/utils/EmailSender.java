package utils;

import org.w3c.dom.Element;

import javax.mail.*;
import javax.mail.internet.*;
import java.util.Properties;

public class EmailSender {

    private static String smtpHost;
    private static String smtpPort;
    private static String username;
    private static String password;
    private static String toEmail;
    private static String toName;

    private static boolean configLoaded = false;

    static {
        try {
            LoadConfig config = new LoadConfig("D:\\DataWareHouse\\src\\main\\java\\config\\config.xml");

            // SMTP
            Element smtp = (Element) config.getXmlDoc().getElementsByTagName("smtp").item(0);
            smtpHost = smtp.getElementsByTagName("host").item(0).getTextContent().trim();
            smtpPort = smtp.getElementsByTagName("port").item(0).getTextContent().trim();

            // From
            Element from = (Element) config.getXmlDoc().getElementsByTagName("from").item(0);
            username = from.getElementsByTagName("address").item(0).getTextContent().trim();
            password = from.getElementsByTagName("password").item(0).getTextContent().trim();

            // To
            Element to = (Element) config.getXmlDoc().getElementsByTagName("to").item(0);
            toEmail = to.getElementsByTagName("address").item(0).getTextContent().trim();
            toName = to.getElementsByTagName("name").item(0).getTextContent().trim();

            configLoaded = true;
            System.out.println("[EmailSender] Email config loaded OK");

        } catch (Exception e) {
            configLoaded = false;
            System.err.println("[EmailSender] Failed to load config: " + e.getMessage());
            e.printStackTrace();

            // fallback
            username = "manhhoang2522004it@gmail.com";
            password = "2522004it";
            toEmail = username;
            toName = "System Administrator";
            smtpHost = "smtp.gmail.com";
            smtpPort = "587";
        }
    }

    public static void sendEmail(String subject, String body) {
        try {
            if (toEmail == null || toEmail.isEmpty()) {
                System.err.println("[EmailSender] Recipient email empty. Using fallback.");
                toEmail = username;
            }

            Properties props = new Properties();
            props.put("mail.smtp.auth", "true");
            props.put("mail.smtp.starttls.enable", "true");
            props.put("mail.smtp.host", smtpHost);
            props.put("mail.smtp.port", smtpPort);

            Session session = Session.getInstance(props, new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(username, password);
                }
            });

            Message msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress(username, "Weather ETL System"));
            msg.setRecipient(Message.RecipientType.TO, new InternetAddress(toEmail, toName));
            msg.setSubject(subject);
            msg.setText(body);

            Transport.send(msg);
            System.out.println("[EmailSender] Email sent to: " + toEmail);

        } catch (Exception e) {
            System.err.println("[EmailSender] Failed to send email: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void sendError(String subject, String message, Exception e) {
        String fullMessage = message + "\n\nError: " + e.getMessage();
        System.err.println(subject + ": " + e.getMessage());
        e.printStackTrace();
        sendEmail(subject, fullMessage);
    }

}
