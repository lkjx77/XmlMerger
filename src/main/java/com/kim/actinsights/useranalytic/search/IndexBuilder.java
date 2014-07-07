package com.kim.actinsights.useranalytic.search;

import com.kim.actinsights.useranalytic.FileManager;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.xml.sax.InputSource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Created by kim on 7/7/14.
 */
public class IndexBuilder {

    /**
     * Transform document to solr supported xml format for indexing
     */

    public static void transform(String path, String indexPath){
        String sourceXML = FileManager.getStringFromFile(path);

        Document document = null;
        try {
            InputSource inputSource = new InputSource(new ByteArrayInputStream(sourceXML.getBytes("utf-8")));
            SAXBuilder saxBuilder = new SAXBuilder();
            document = saxBuilder.build(inputSource);
            XPathFactory xFactory = XPathFactory.instance();

            // id: /profile/id
            Element exp_id = xFactory.compile("//id", Filters.element()).evaluateFirst(document);
            String uid = exp_id.getValue();

            Element index = new Element("add");
            Document indexDoc = new Document(index);

            Element doc = new Element("doc");
            doc.addContent(new Element("field").setAttribute("name", "id").setText(uid));

            // add basic info /profile/tags[1]
            XPathExpression<Element> expr_basic = xFactory.compile("//tags[1]/tag", Filters.element());
            List<Element> basic = expr_basic.evaluate(document);
            for (Element linkElement : basic) {
                for(Attribute attr: linkElement.getAttributes()){
                    doc.addContent(new Element("field").setAttribute("name", attr.getValue()).setText(linkElement.getValue()));
                }
            }

            // add category info
            buildTag(doc, "//tags[2]/tag", "category", document);
            // add brand info
            buildTag(doc, "//tags[3]/tag", "brand", document);
            // add status info
            buildTag(doc, "//tags[4]/tag", "status", document);

            index.addContent(doc);

            XMLOutputter xmlOutput = new XMLOutputter();

            xmlOutput.setFormat(Format.getPrettyFormat());
            xmlOutput.output(indexDoc, new FileWriter(indexPath));

        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void buildTag(Element doc, String xpath, String attrName, Document document){
        XPathFactory xFactory = XPathFactory.instance();
        XPathExpression<Element> expr = xFactory.compile(xpath, Filters.element());
        List<Element> tags = expr.evaluate(document);

        StringBuilder sb = new StringBuilder();
        for (Element tag : tags) {
            sb.append(tag.getAttributeValue("name") + ",");
        }
        sb.deleteCharAt(sb.toString().length() - 1);
        doc.addContent(new Element("field").setAttribute("name", attrName).setText(sb.toString()));

    }

    public static void main(String[] args) {

        IndexBuilder.transform("data/user1.xml", "index/user1.xml");
    }
}
