USE test;

DROP TABLE IF EXISTS t;

CREATE TABLE t (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(42),
    birth DATE,
    cash DECIMAL(18, 4),
    last_visit DATETIME,
    gender CHAR(1)
);

INSERT INTO
    t
VALUES
    (
        1,
        '233',
        '2021-09-30',
        12.34,
        '2021-10-09 16:33:33',
        'M'
    );
