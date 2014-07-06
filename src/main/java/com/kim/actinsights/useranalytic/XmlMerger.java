package com.kim.actinsights.useranalytic;

import com.kim.actsights.utils.XmlMergerHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.xml.sax.InputSource;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.List;

/**
 * Created by jizhe on 7/4/2014.
 */
public class XmlMerger {
    public static void main(String[] args) throws Exception {
        FSDataOutputStream out = null;

        try {

            String sourceXML = FileManager.getStringFromFile("./002/user5.xml");
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
            System.out.println("uid " + exp_id.getValue());

            String hdfsUrl = "hdfs://localhost:9000";
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(hdfsUrl), conf);
            out = fs.create(new Path("/output/002/" + uid +".xml"));

            out.write(("uid " + exp_id.getValue() + "\n").getBytes());

            // basic: /profile/tags[1]
            XPathExpression<Element> expr_basic = xFactory.compile("//tags[1]/tag", Filters.element());
            List<Element> basic = expr_basic.evaluate(document);
            String basicProfile = "";
            for (Element linkElement : basic) {
                for(Attribute attr: linkElement.getAttributes()){
                    basicProfile = uid + "#" + "basic" + "#" + attr.getValue() + " " + linkElement.getValue() + "\n";
                    out.write(basicProfile.getBytes());
//                    System.out.println(uid + "#" + "basic" + "#" + attr.getValue() + " " + linkElement.getValue());
                }
            }

            // category: /profile/tags[2]
            // brand: /profile/tags[3]
            // status: /profile/tags[4]
            // Step2: second Map Reduce
            // out put
            // key: id  value: basic#age 30
            // key: id  value: basic#Food 0.6
            XmlMergerHelper.splitAsMap(out, document, "//tags[2]/tag", uid, "category");
            XmlMergerHelper.splitAsMap(out, document, "//tags[3]/tag", uid, "brand");
            XmlMergerHelper.splitAsMap(out, document, "//tags[4]/tag", uid, "status");

            // Final step
            // Build user profile xml



        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            out.close();
        }
    }


}
