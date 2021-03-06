package com.kim.actsights.utils;

import com.google.common.base.Splitter;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;

/**
 * Created by kim on 7/6/14.
 */
public class XmlSplitter extends DoFn<String, String> {
    private static final Splitter SPLITTER = Splitter.onPattern("\\<[/w+]>[^<|^>]+<[/w+]>").omitEmptyStrings();
    /**
     * Maps the given input into an instance of the output type.
     */
//    public String map(String input){
//
//
//    }

    @Override
    public void process(String line, Emitter<String> emitter) {
        for (String word : SPLITTER.split(line)) {
            emitter.emit(word);
        }
    }
}
