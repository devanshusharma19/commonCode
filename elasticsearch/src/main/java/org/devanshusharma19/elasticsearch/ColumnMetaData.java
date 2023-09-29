package org.devanshusharma19.elasticsearch;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/***
 * get column metadata from nested object given by elastic search
 */
public class ColumnMetaData {

    /**
     * call the function using getColumnMetaData(new HashMap<>(), stringObjectMap)
     *
     * @param columnObject empty hashmap object for storing flatter metadata
     * @param metaDataMap  need elastic metadata object request response.
     * @return Map<String, Object> flatten metadata
     */
    private Map<String, Object> getColumnMetaData(Map<String, Object> columnObject, Map<String, Object> metaDataMap) {
        if (metaDataMap.containsKey("type")) {
            Object type = metaDataMap.get("type");
            if (!(type instanceof Map)) {
                metaDataMap.put("get_data_type", true);
                return metaDataMap;
            }
        }
        if (metaDataMap.containsKey("properties")) {
            Map<String, Object> properties = (Map<String, Object>) metaDataMap.get("properties");
            return getColumnMetaData(new HashMap<>(), properties);
        } else {
            Set<Map.Entry<String, Object>> entrySets = metaDataMap.entrySet();
            entrySets.forEach(entrySet -> {
                String key = entrySet.getKey();
                if (!key.equalsIgnoreCase("_class")) {
                    Object value = entrySet.getValue();
                    if (value instanceof Map) {
                        Map<String, Object> objectMap = (Map<String, Object>) value;
                        objectMap = getColumnMetaData(columnObject, objectMap);
                        if (objectMap.containsKey("get_data_type")) {
                            columnObject.put(key, objectMap.get("type"));
                        } else {
                            columnObject.put(key, objectMap);
                        }
                    }
                }
            });
        }
        return columnObject;
    }
}
