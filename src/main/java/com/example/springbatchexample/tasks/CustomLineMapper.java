package com.example.springbatchexample.tasks;

import com.example.springbatchexample.models.DataRecord;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

public class CustomLineMapper extends DefaultLineMapper<DataRecord> {

    final ArrayList<String> fields;
    final String delimiter;

    public CustomLineMapper(ArrayList<String> fields, String delimiter) {
        this.fields = fields;
        this.delimiter = delimiter;
    }

    @Override
    public DataRecord mapLine(String line, int lineNumber) throws Exception {
        DataRecord record = new DataRecord();
        record.setLineNumber(lineNumber);
        record.setDataMap(lineToMap(fields, line, delimiter));
        return record;
        //return super.mapLine(line, lineNumber);
    }

    public static HashMap<String, String> lineToMap(ArrayList<String> fields, String line, String delimiter) {
        HashMap<String, String> dataMap = new HashMap<>();
        String[] lineArr = line.split(Pattern.quote(delimiter) + "(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        for(int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);
            if(i < lineArr.length) {
                dataMap.put(field, lineArr[i]);
            } else {
                dataMap.put(field, null);
            }
        }
        return dataMap;
    }

}