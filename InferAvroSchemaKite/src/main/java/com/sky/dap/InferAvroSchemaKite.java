package com.sky.dap;

import org.kitesdk.data.spi.JsonUtil;

/**
 * Created by dap on 6/26/18.
 */
public class InferAvroSchemaKite {

    public static void main(String[] args) {
        // Prints "Hello, World" to the terminal window.
        System.out.println("Hello, World");

        String json = "{\n" +
                "    \"id\": 1,\n" +
                "    \"name\": \"A green door\",\n" +
                "    \"price_net\": 12.50,\n" +
                "    \"tags\": [\"home\", \"green\"]\n" +
                "}\n"
                ;

        String avroSchema = JsonUtil.inferSchema(JsonUtil.parse(json), "myschema").toString();
        System.out.println(avroSchema);

    }

}
