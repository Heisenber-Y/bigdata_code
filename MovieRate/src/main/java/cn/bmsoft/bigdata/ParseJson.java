package cn.bmsoft.bigdata;

import org.apache.hadoop.hive.ql.exec.UDF;
import parquet.org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class ParseJson extends UDF {

    public String evaluate(String line){

        ObjectMapper objectMapper = new ObjectMapper();
        MovieRate bean = null;
        try {
            bean = objectMapper.readValue(line, MovieRate.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  bean.toString();
    }




}
