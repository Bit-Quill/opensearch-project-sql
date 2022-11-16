/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.mapping;

import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.sql.data.type.ExprType;

@AllArgsConstructor
public class MappingEntry {

  @Getter
  private String fieldType;

  @Getter
  private String formats;

  @Getter
  @Setter
  private ExprType dataType;

  public MappingEntry(String fieldType) {
    this(fieldType, null, null);
  }

  public List<String> getFormatList() {
    if (formats == null || formats.isEmpty()) {
      return List.of();
    }
    return Arrays.stream(formats.split("\\|\\|")).map(String::trim).collect(Collectors.toList());
  }

  public List<DateFormatter> getNamedFormatters() {
    return getFormatList().stream().filter(f -> {
          try {
            DateTimeFormatter.ofPattern(f);
            return false;
          } catch (Exception e) {
            return true;
          }
        })
        .map(DateFormatter::forPattern).collect(Collectors.toList());
  }

  public List<DateTimeFormatter> getRegularFormatters() {
    return getFormatList().stream().map(f -> {
          try {
            return DateTimeFormatter.ofPattern(f);
          } catch (Exception e) {
            return null;
          }
        })
        .filter(Objects::nonNull).collect(Collectors.toList());
  }
}
