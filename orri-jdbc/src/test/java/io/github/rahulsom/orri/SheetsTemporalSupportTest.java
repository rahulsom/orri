package io.github.rahulsom.orri;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.junit.jupiter.api.Test;

class SheetsTemporalSupportTest {
    @Test
    void convertsDateSerials() {
        CellValue cellValue = SheetsTemporalSupport.fromSerial(46050d, ColumnType.DATE, "2026-01-28");

        assertThat(cellValue.kind()).isEqualTo(ValueKind.DATE);
        assertThat(cellValue.typedValue()).isEqualTo(LocalDate.of(2026, 1, 28));
        assertThat(SheetsTemporalSupport.toSerial(cellValue.typedValue(), ColumnType.DATE))
                .isEqualTo(new BigDecimal("46050"));
    }

    @Test
    void convertsTimeSerials() {
        CellValue cellValue = SheetsTemporalSupport.fromSerial(0.3857638888888889d, ColumnType.TIME, "09:15:30");

        assertThat(cellValue.kind()).isEqualTo(ValueKind.TIME);
        assertThat(cellValue.typedValue()).isEqualTo(LocalTime.of(9, 15, 30));
        assertThat(SheetsTemporalSupport.toSerial(cellValue.typedValue(), ColumnType.TIME))
                .isEqualTo(new BigDecimal("0.385763888888889"));
    }

    @Test
    void convertsTimestampSerials() {
        CellValue cellValue =
                SheetsTemporalSupport.fromSerial(46119.3857638888888889d, ColumnType.TIMESTAMP, "2026-04-07 09:15:30");

        assertThat(cellValue.kind()).isEqualTo(ValueKind.TIMESTAMP);
        assertThat(cellValue.typedValue()).isEqualTo(LocalDateTime.of(2026, 4, 7, 9, 15, 30));
        assertThat(SheetsTemporalSupport.toSerial(cellValue.typedValue(), ColumnType.TIMESTAMP))
                .isEqualTo(new BigDecimal("46119.385763888888889"));
    }
}
