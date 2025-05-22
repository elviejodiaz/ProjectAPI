CREATE TABLE dbo.DimDate (
    DateKey             INT         NOT NULL PRIMARY KEY, -- yyyymmdd
    FullDate            DATE        NOT NULL,
    Day                 TINYINT     NOT NULL,
    DaySuffix           CHAR(4)     NOT NULL,
    Weekday             TINYINT     NOT NULL,
    WeekdayName         VARCHAR(10) NOT NULL,
    IsWeekend           BIT         NOT NULL,
    DOWInMonth          TINYINT     NOT NULL,
    DayOfYear           SMALLINT    NOT NULL,
    WeekOfMonth         TINYINT     NOT NULL,
    WeekOfYear          TINYINT     NOT NULL,
    Month               TINYINT     NOT NULL,
    MonthName           VARCHAR(10) NOT NULL,
    Quarter             TINYINT     NOT NULL,
    QuarterName         VARCHAR(10) NOT NULL,
    Year                INT         NOT NULL,
    MMYYYY              CHAR(6)     NOT NULL,
    FirstDayOfMonth     DATE        NOT NULL,
    LastDayOfMonth      DATE        NOT NULL,
    IsLeapYear          BIT         NOT NULL
);
GO