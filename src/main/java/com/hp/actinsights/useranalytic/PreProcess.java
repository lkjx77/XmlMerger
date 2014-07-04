package com.hp.actinsights.useranalytic;

import com.hp.actsights.utils.DataProcessUtil;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by kim on 7/2/14.
 */
public class PreProcess {

    public static void main(String[] args) {

        try {
            Configuration conf = new Configuration(true);
//        conf.addResource("/home/kim/IdeaProjects/actinsights-useranalytic/data");

            DataProcessUtil dpu = new DataProcessUtil(conf);
//        dpu.copyToHDFS();
            dpu.testUploadLocalFile();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
