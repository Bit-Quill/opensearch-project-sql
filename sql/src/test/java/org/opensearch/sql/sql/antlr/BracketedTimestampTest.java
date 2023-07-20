package org.opensearch.sql.sql.antlr;

import org.junit.jupiter.api.Test;

public class BracketedTimestampTest extends SQLParserTest {
    @Test
    void d_test() {
        acceptQuery("SELECT {d '2001-05-07'}");
    }

    @Test
    void date_test() {
        acceptQuery("SELECT {date '2001-05-07'}");
    }

    @Test
    void t_test() {
        acceptQuery("SELECT {t '10:11:12'}");
    }

    @Test
    void time_test() {
        acceptQuery("SELECT {time '10:11:12'}");
    }

    @Test
    void ts_test() {
        acceptQuery("SELECT {ts '2001-05-07 10:11:12'}");
    }

    @Test
    void timestamp_test() {
        acceptQuery("SELECT {timestamp '2001-05-07 10:11:12'}");
    }

    @Test
    void invalid_time_test() {
        rejectQuery("SELECT {time '2001-05-07'}");
    }

    @Test
    void invalid_t_test() {
        rejectQuery("SELECT {t '2001-05-07'}");
    }

    @Test
    void invalid_date_test() {
        rejectQuery("SELECT {date '10:11:12'}");
    }

    @Test
    void invalid_d_test() {
        rejectQuery("SELECT {d '10:11:12'}");
    }

    @Test
    void invalid_timestamp_test() {
        rejectQuery("SELECT {timestamp 'invalid'}");
    }

    @Test
    void invalid_ts_test() {
        rejectQuery("SELECT {ts 'invalid'}");
    }
}
