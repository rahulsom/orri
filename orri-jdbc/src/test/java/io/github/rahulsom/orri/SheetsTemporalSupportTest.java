package io.github.rahulsom.orri;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.junit.jupiter.api.Test;

class SheetsTemporalSupportTest {
    @Test
    void convertsDateSerials() {
        CellValue cellValue = SheetsTemporalSupport.fromSerial(46050d, ColumnType.DATE, "2026-01-28");

        assertEquals(ValueKind.DATE, cellValue.kind());
        assertEquals(LocalDate.of(2026, 1, 28), cellValue.typedValue());
        assertEquals(new BigDecimal("46050"), SheetsTemporalSupport.toSerial(cellValue.typedValue(), ColumnType.DATE));
    }

    @Test
    void convertsTimeSerials() {
        CellValue cellValue = SheetsTemporalSupport.fromSerial(0.3857638888888889d, ColumnType.TIME, "09:15:30");

        assertEquals(ValueKind.TIME, cellValue.kind());
        assertEquals(LocalTime.of(9, 15, 30), cellValue.typedValue());
        assertEquals(
                new BigDecimal("0.385763888888889"),
                SheetsTemporalSupport.toSerial(cellValue.typedValue(), ColumnType.TIME));
    }

    @Test
    void convertsTimestampSerials() {
        CellValue cellValue =
                SheetsTemporalSupport.fromSerial(46119.3857638888888889d, ColumnType.TIMESTAMP, "2026-04-07 09:15:30");

        assertEquals(ValueKind.TIMESTAMP, cellValue.kind());
        assertEquals(LocalDateTime.of(2026, 4, 7, 9, 15, 30), cellValue.typedValue());
        assertEquals(
                new BigDecimal("46119.385763888888889"),
                SheetsTemporalSupport.toSerial(cellValue.typedValue(), ColumnType.TIMESTAMP));
    }
}
