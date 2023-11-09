package org.apache.hudi.examples.spark;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class NonDefaultPartitionValueExtractor implements PartitionValueExtractor {
    private static final long serialVersionUID = -5945935016448019691L;

    @Override
    public List<String> extractPartitionValuesInPath(String partitionPath) {
        String[] splits = partitionPath.split("/");
        return Arrays.stream(splits).map(s -> {
            if (s.contains("=")) {
                String[] moreSplit = s.split("=");
                ValidationUtils.checkArgument(moreSplit.length == 2, "Partition Field (" + s + ") not in expected format");
                return moreSplit[1];
            } else if (s.equals("__HIVE_DEFAULT_PARTITION__")) {
                return "0";
            } else {
                return s;
            }
        }).collect(Collectors.toList());
    }
}
