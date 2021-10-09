USE test;

CREATE TABLE t (
    id INT,
    name VARCHAR(42),
    birth DATE,
    cash DECIMAL(6, 2),
    last_visit DATETIME
);

INSERT INTO
    t
VALUES
    (
        1,
        '233',
        '2021-09-30',
        12.34,
        '2021-10-09 16:33:33'
    );
