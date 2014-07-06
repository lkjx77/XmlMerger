package com.kim.actsights.utils;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.filter.Filters;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;

import java.util.List;

/**
 * Created by kim on 7/6/14.
 */
public class XmlMergerHelper {

    public static void splitAsMap(FSDataOutputStream out, Document document, String path, String uid, String group){
        try{
            XPathFactory xFactory = XPathFactory.instance();
            XPathExpression<Element> expr_category = xFactory.compile(path, Filters.element());
            List<Element> category = expr_category.evaluate(document);
            StringBuilder sb = new StringBuilder();
            for (Element linkElement : category) {
                sb.append(uid + "#" + group + "#");
                for(Attribute attr: linkElement.getAttributes()){
                    sb.append(attr.getValue());
                    if (attr.getName() != "weight"){
                        sb.append(" ");
                    }

                }
                sb.append("\n");
                out.write(sb.toString().getBytes());
                sb.delete(0, sb.length());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
