package com.hxr.hadoop.mr.pagerank;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.stream.Stream;


public class Node {
    private double pageRank = 1.0;
    private String[] adjacentNodeNames; // [B, D]
    private static final Log log = LogFactory.getLog(Node.class);

    public static void main(String[] args) {
        Node node = Node.fromMR("1.0","A\tB D");
        System.out.println(node.toString());
    }

    public static final char fieldSeperator = '\t';

    public double getPageRank() {
        return pageRank;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }

    public String[] getAdjacentNodeNames() {
        return adjacentNodeNames;
    }

    public void setAdjacentNodeNames(String[] adjacentNodeNames) {
        this.adjacentNodeNames = adjacentNodeNames;
    }

    public boolean containAdjacentNodes() {
        return adjacentNodeNames != null && adjacentNodeNames.length > 0;
    }

    public static Node fromMR(String valueStr){

        log.info("---------!!: "+valueStr);
        Node node = new Node();
        double newpagerank;
        String[] values = StringUtils.split(valueStr, fieldSeperator);
        if(values.length>1){
            newpagerank = Double.parseDouble(values[0]);
            String[] adjacentNodeNames = StringUtils.split(values[1], ' ');
            node.setPageRank(newpagerank);
            node.setAdjacentNodeNames(adjacentNodeNames);
            return node;
        }else{
            try{
                newpagerank = Double.parseDouble(values[0]);
                node.setPageRank(newpagerank);
                node.setAdjacentNodeNames(null);
                return node;
            }catch (Exception e){
                node.setAdjacentNodeNames(StringUtils.split(values[0], ' '));
                return node;
            }
        }

    }

    public static Node fromMR(String i, String valueStr){

            log.info("---------!!: "+valueStr);
            Node node = new Node();
            node.setPageRank(Double.parseDouble(i));
            //String[] split = StringUtils.split(valueStr, fieldSeperator);
            String[] adjacentNodeNames = StringUtils.split(valueStr, ' ');
            node.setAdjacentNodeNames(adjacentNodeNames);
            return node;
    }

    @Override
    public String toString() {
        String[] strArray = new String[2];
        strArray[0] = String.valueOf(pageRank);
        strArray[1] = StringUtils.join(adjacentNodeNames,' ');
        String returnVal =  StringUtils.join(strArray,fieldSeperator);
        return returnVal;
    }
}
