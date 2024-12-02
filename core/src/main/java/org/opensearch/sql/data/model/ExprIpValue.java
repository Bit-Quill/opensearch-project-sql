/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.IPAddressStringParameters;
import inet.ipaddr.ipv4.IPv4Address;
import inet.ipaddr.ipv6.IPv6Address;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;

/** Expression IP Address Value. */
public class ExprIpValue extends AbstractExprValue {
  private final IPAddress value;

  private static final IPAddressStringParameters validationOptions =
      new IPAddressStringParameters.Builder()
          .allowEmpty(false)
          .allowMask(false)
          .allowPrefix(false)
          .setEmptyAsLoopback(false)
          .allow_inet_aton(false)
          .allowSingleSegment(false)
          .toParams();

  public ExprIpValue(String s) {
    value = stringToIpAddress(s);
  }

  @Override
  public String value() {
    return value.toCanonicalString();
  }

  @Override
  public ExprType type() {
    return ExprCoreType.IP;
  }

  @Override
  public int compare(ExprValue other) {
    IPAddress otherValue =
        other instanceof ExprIpValue exprIpValue
            ? exprIpValue.value
            : stringToIpAddress(other.stringValue());

    // Map IPv4 addresses to IPv6 for comparison
    IPv6Address ipv6Value = toIPv6Address(value);
    IPv6Address otherIpv6Value = toIPv6Address(otherValue);

    return ipv6Value.compareTo(otherIpv6Value);
  }

  @Override
  public boolean equal(ExprValue other) {
    return compare(other) == 0;
  }

  @Override
  public String toString() {
    return String.format("IP %s", value());
  }

  /** Returns the {@link IPAddress} corresponding to the given {@link String}. */
  private static IPAddress stringToIpAddress(String s) {
    try {
      IPAddress address = new IPAddressString(s, validationOptions).toAddress();
      return address.isIPv4Convertible() ? address.toIPv4() : address;
    } catch (AddressStringException e) {
      final String errorFormatString = "IP address '%s' is not valid. Error details: %s";
      throw new SemanticCheckException(String.format(errorFormatString, s, e.getMessage()));
    }
  }

  /** Returns the {@link IPv6Address} corresponding to the given {@link IPAddress}. */
  private static IPv6Address toIPv6Address(IPAddress ipAddress) {
    return ipAddress instanceof IPv4Address iPv4Address
        ? iPv4Address.toIPv6()
        : (IPv6Address) ipAddress;
  }
}
