package io.github.rahulsom.orri;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;

final class SheetsTemporalSupport {
    private static final LocalDate SERIAL_EPOCH = LocalDate.of(1899, 12, 30);
    private static final BigDecimal NANOS_PER_DAY = BigDecimal.valueOf(24L * 60L * 60L * 1_000_000_000L);
    private static final int SERIAL_SCALE = 15;

    private SheetsTemporalSupport() {}

    static ColumnType columnType(String numberFormatType) {
        if (numberFormatType == null) {
            return null;
        }

        return switch (numberFormatType) {
            case "DATE" -> ColumnType.DATE;
            case "TIME" -> ColumnType.TIME;
            case "DATE_TIME" -> ColumnType.TIMESTAMP;
            default -> null;
        };
    }

    static CellValue fromSerial(double serialValue, ColumnType columnType, String displayValue) {
        BigDecimal serial = BigDecimal.valueOf(serialValue);
        return switch (columnType) {
            case DATE -> new CellValue(asDate(serial), displayValue, ValueKind.DATE);
            case TIME -> new CellValue(asTime(serial), displayValue, ValueKind.TIME);
            case TIMESTAMP -> new CellValue(asTimestamp(serial), displayValue, ValueKind.TIMESTAMP);
            default -> throw new IllegalArgumentException("Unsupported temporal column type: " + columnType);
        };
    }

    static BigDecimal toSerial(Object value, ColumnType columnType) {
        return switch (columnType) {
            case DATE -> BigDecimal.valueOf(daysSinceEpoch(asLocalDate(value)));
            case TIME ->
                BigDecimal.valueOf(asLocalTime(value).toNanoOfDay())
                        .divide(NANOS_PER_DAY, SERIAL_SCALE, RoundingMode.HALF_UP)
                        .stripTrailingZeros();
            case TIMESTAMP -> {
                LocalDateTime timestamp = asLocalDateTime(value);
                BigDecimal dayPortion = BigDecimal.valueOf(daysSinceEpoch(timestamp.toLocalDate()));
                BigDecimal timePortion = BigDecimal.valueOf(
                                timestamp.toLocalTime().toNanoOfDay())
                        .divide(NANOS_PER_DAY, SERIAL_SCALE, RoundingMode.HALF_UP);
                yield dayPortion.add(timePortion).stripTrailingZeros();
            }
            default -> throw new IllegalArgumentException("Unsupported temporal column type: " + columnType);
        };
    }

    static String numberFormatType(ColumnType columnType) {
        return switch (columnType) {
            case DATE -> "DATE";
            case TIME -> "TIME";
            case TIMESTAMP -> "DATE_TIME";
            default -> null;
        };
    }

    static String numberFormatPattern(ColumnType columnType) {
        return switch (columnType) {
            case DATE -> "yyyy-mm-dd";
            case TIME -> "hh:mm:ss";
            case TIMESTAMP -> "yyyy-mm-dd hh:mm:ss";
            default -> null;
        };
    }

    static LocalDate asLocalDate(Object value) {
        if (value instanceof LocalDate localDate) {
            return localDate;
        }
        if (value instanceof java.sql.Date sqlDate) {
            return sqlDate.toLocalDate();
        }
        throw new IllegalArgumentException("Unsupported DATE value: " + value);
    }

    static LocalTime asLocalTime(Object value) {
        if (value instanceof LocalTime localTime) {
            return localTime;
        }
        if (value instanceof java.sql.Time sqlTime) {
            return sqlTime.toLocalTime();
        }
        throw new IllegalArgumentException("Unsupported TIME value: " + value);
    }

    static LocalDateTime asLocalDateTime(Object value) {
        if (value instanceof LocalDateTime localDateTime) {
            return localDateTime;
        }
        if (value instanceof java.sql.Timestamp sqlTimestamp) {
            return sqlTimestamp.toLocalDateTime();
        }
        throw new IllegalArgumentException("Unsupported TIMESTAMP value: " + value);
    }

    private static LocalDate asDate(BigDecimal serial) {
        return SERIAL_EPOCH.plusDays(serial.setScale(0, RoundingMode.FLOOR).longValue());
    }

    private static LocalTime asTime(BigDecimal serial) {
        return LocalTime.ofNanoOfDay(nanosWithinDay(serial));
    }

    private static LocalDateTime asTimestamp(BigDecimal serial) {
        long wholeDays = serial.setScale(0, RoundingMode.FLOOR).longValue();
        long nanos = nanosWithinDay(serial);
        if (nanos == 0L && serial.compareTo(BigDecimal.valueOf(wholeDays)) > 0) {
            wholeDays++;
        }
        return SERIAL_EPOCH.plusDays(wholeDays).atTime(LocalTime.ofNanoOfDay(nanos));
    }

    private static long nanosWithinDay(BigDecimal serial) {
        long wholeDays = serial.setScale(0, RoundingMode.FLOOR).longValue();
        BigDecimal dayFraction = serial.subtract(BigDecimal.valueOf(wholeDays));
        long nanos = dayFraction
                .multiply(NANOS_PER_DAY)
                .setScale(0, RoundingMode.HALF_UP)
                .longValue();
        nanos = Math.round(nanos / 1_000d) * 1_000L;
        return nanos == NANOS_PER_DAY.longValue() ? 0L : nanos;
    }

    private static long daysSinceEpoch(LocalDate localDate) {
        return ChronoUnit.DAYS.between(SERIAL_EPOCH, localDate);
    }
}
