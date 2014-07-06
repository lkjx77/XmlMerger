package com.hp.actinsights.useranalytic;

import com.ganesh.transformer.FileManager;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.xpath.XPath;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.jdom2.xpath.XPathHelper;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by jizhe on 7/4/2014.
 */
public class XmlMerger {
    public static void main(String[] args) throws Exception {

        try {
            String sourceXML = FileManager.getStringFromFile("User1.xml");
            InputSource inputSource = new InputSource(new ByteArrayInputStream(sourceXML.getBytes("utf-8")));
            SAXBuilder saxBuilder = new SAXBuilder();
            Document document = saxBuilder.build(inputSource);

            Element element = document.getRootElement();

            // Step1: first Map Reduce
            // get basic info, category info, brand info, status info
            // out put
            // key:id#basic#age value:30
            // key:id#category#Food value:0.6
            // get mean value of the weight by reduce step

            // id: /profile/id
            XPathFactory xFactory = XPathFactory.instance();
            Element exp_id = xFactory.compile("//id", Filters.element()).evaluateFirst(document);
            String uid = exp_id.getValue();
            System.out.println("uid#" + exp_id.getValue());

            // basic: /profile/tags[1]
            XPathExpression<Element> expr_basic = xFactory.compile("//tags[1]/tag", Filters.element());
            List<Element> basic = expr_basic.evaluate(document);
            for (Element linkElement : basic) {
                for(Attribute attr: linkElement.getAttributes()){
                    System.out.println("uid#" + attr.getValue() + "#" + linkElement.getValue());
                }
            }

            // category: /profile/tags[2]
            XPathExpression<Element> expr_category = xFactory.compile("//tags[2]/tag", Filters.element());
            List<Element> category = expr_category.evaluate(document);
            for (Element linkElement : category) {
                for(Attribute attr: linkElement.getAttributes()){
                    System.out.println(attr.getValue());
                }
            }

            // brand: /profile/tags[3]
            // status: /profile/tags[4]
            // Step2: second Map Reduce
            // out put
            // key: id  value: basic#age#30
            // key: id  value: basic#Food#0.6

            // Final step
            // Build user profile xml



        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
