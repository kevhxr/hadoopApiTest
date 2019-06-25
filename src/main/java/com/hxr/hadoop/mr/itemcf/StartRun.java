package com.hxr.hadoop.mr.itemcf;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

public class StartRun {


    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.app-submission.cross-platform", "true");
        configuration.set("mapreduce.framework.name", "local");

        Map<String,String>paths = new HashedMap();
        paths.put("Step1Input","/data/itemcf/input/");
        paths.put("Step1Output","/data/itemcf/output/step1");
        paths.put("Step2Input",paths.get("Step1Output"));
        paths.put("Step2Output","/data/itemcf/output/step2");
        paths.put("Step3Input",paths.get("Step2Output"));
        paths.put("Step3Output","/data/itemcf/output/step3");
        paths.put("Step4Input1",paths.get("Step2Output"));
        paths.put("Step4Input2",paths.get("Step3Output"));
        paths.put("Step4Output","/data/itemcf/output/step4");
        paths.put("Step5Input",paths.get("Step4Output"));
        paths.put("Step5Output","/data/itemcf/output/step5");

        //Step1.run(configuration,paths);
        //Step2.run(configuration,paths);
        //Step3.run(configuration,paths);
        //Step4.run(configuration,paths);
        //Step5.run(configuration,paths);
    }

    public static Map<String, Integer> R = new HashMap<>();
    static {
        R.put("click",1);
        R.put("collect",2);
        R.put("cart",3);
        R.put("alipay",4);
    }
}
